import io
from datetime import datetime, timezone
from textwrap import dedent
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import NoCredentialsError
from fastapi import HTTPException
from fastapi.testclient import TestClient

from bdi_api.s7.exercise import (
    DBCredentials,
    create_table_if_not_exists,
    get_db_connection,
    get_db_credentials,
    insert_data_to_postgres,
    refine_data,
)
from bdi_api.s7.s7_helper import process_batch_s7, process_s3_files


class TestS7Student:

    def test_get_db_credentials(self, client: TestClient):
        mock_credentials = DBCredentials(
            host="test_host",
            port=5432,
            username="test_user",
            password="test_password",
            database="test_db"
        )
        with patch("bdi_api.s7.exercise.DBCredentials", return_value=mock_credentials):
            credentials = get_db_credentials()
            assert credentials.host == "test_host"
            assert credentials.port == 5432
            assert credentials.username == "test_user"
            assert credentials.password == "test_password"
            assert credentials.database == "test_db"

    def test_get_db_connection(self, client: TestClient):
        mock_credentials = get_db_credentials()
        mock_credentials.host = "test_host"
        mock_credentials.port = 5432
        mock_credentials.username = "test_user"
        mock_credentials.password = "test_password"
        mock_credentials.database = "test_db"

        with (
            patch("bdi_api.s7.exercise.get_db_credentials", return_value=mock_credentials),
            patch("bdi_api.s7.exercise.psycopg2.connect") as mock_connect
        ):
            mock_connect.return_value = MagicMock()
            conn = get_db_connection()
            mock_connect.assert_called_once_with(
                host="test_host",
                port=5432,
                user="test_user",
                password="test_password",
                dbname="test_db"
            )
            assert conn == mock_connect.return_value

    def test_create_table_if_not_exists(self, client: TestClient):
        """Test that the function executes the correct SQL command and commits."""

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        with patch("bdi_api.s7.exercise.get_db_connection", return_value=mock_conn):
            create_table_if_not_exists()

            expected_sql = """
            CREATE TABLE IF NOT EXISTS aircraft_data (
                id SERIAL PRIMARY KEY,
                icao VARCHAR(10),
                registration VARCHAR(20),
                type VARCHAR(10),
                lat FLOAT,
                lon FLOAT,
                max_altitude_baro INTEGER,
                max_ground_speed FLOAT,
                had_emergency VARCHAR(10),
                timestamp TIMESTAMP,
                CONSTRAINT aircraft_unique UNIQUE (icao, timestamp)
            );
            """.strip()

            actual_sql = mock_cursor.execute.call_args[0][0].strip()
            assert " ".join(actual_sql.split()) == " ".join(expected_sql.split()), (
                f"SQL Mismatch!\nExpected:\n{expected_sql}\n\nActual:\n{actual_sql}"
            )
            mock_conn.commit.assert_called_once()

    def test_insert_data_to_postgres(self, client: TestClient):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        sample_data = [
            {
                "icao": "ABC123",
                "registration": "N12345",
                "type": "A320",
                "lat": 40.7128,
                "lon": -74.0060,
                "max_altitude_baro": 35000,
                "max_ground_speed": 450.0,
                "had_emergency": "NO",
                "timestamp": 1710805200,
            }
        ]

        with patch("bdi_api.s7.exercise.get_db_connection", return_value=mock_conn), \
                patch("psycopg2.extras.execute_values") as mock_execute_values:

            insert_data_to_postgres(sample_data)
            mock_cursor.execute.assert_not_called()
            mock_conn.commit.assert_called_once()
            mock_execute_values.assert_called_once()

            args, kwargs = mock_execute_values.call_args

            expected_sql = """
                INSERT INTO aircraft_data (
                    icao, registration, type, lat, lon,
                    max_altitude_baro, max_ground_speed, had_emergency, timestamp
                )
                VALUES %s
                ON CONFLICT (icao, timestamp)
                DO UPDATE SET
                    registration = EXCLUDED.registration,
                    type = EXCLUDED.type,
                    lat = EXCLUDED.lat,
                    lon = EXCLUDED.lon,
                    max_altitude_baro = EXCLUDED.max_altitude_baro,
                    max_ground_speed = EXCLUDED.max_ground_speed,
                    had_emergency = EXCLUDED.had_emergency,
                    timestamp = EXCLUDED.timestamp;
            """.strip()

            actual_sql = args[1].strip()

            assert " ".join(actual_sql.split()) == " ".join(expected_sql.split()), (
                f"SQL Mismatch!\nExpected:\n{expected_sql}\n\nActual:\n{actual_sql}"
            )

            records_inserted = args[2]
            expected_timestamp = datetime.fromtimestamp(1710805200, tz=timezone.utc)

            assert records_inserted == [
                ("ABC123", "N12345", "A320", 40.7128, -74.0060, 35000, 450.0, "NO", expected_timestamp)
            ]

    def test_insert_data_to_postgres_empty(self, client: TestClient):
        mock_conn = MagicMock()

        with patch("bdi_api.s7.exercise.get_db_connection", return_value=mock_conn):
            insert_data_to_postgres([])

            mock_conn.cursor.assert_not_called()
            mock_conn.commit.assert_not_called()

    def test_prepare_no_valid_aircraft_data(self, client: TestClient):
        with (
            patch("bdi_api.s7.exercise.get_db_credentials") as mock_db_creds,
            patch("bdi_api.s7.exercise.get_db_connection") as mock_db_conn,
            patch("bdi_api.s7.exercise.refine_data") as mock_refine_data,
        ):

            mock_db_creds.return_value.host = "localhost"
            mock_db_creds.return_value.port = 5432
            mock_db_creds.return_value.username = "test_user"
            mock_db_creds.return_value.password = "test_pass"
            mock_db_creds.return_value.database = "test_db"

            mock_db_conn.return_value.cursor.return_value.__enter__.return_value.execute.return_value = None
            mock_refine_data.return_value = []

            response = client.post("/api/s7/aircraft/prepare")

            assert response.status_code == 200
            assert response.json() == "No valid records found. Skipping database insertion."

            mock_refine_data.assert_called_once()

    def test_prepare_successful(self, client: TestClient):
        with (
            patch("bdi_api.s7.exercise.create_table_if_not_exists") as mock_create_table,
            patch("bdi_api.s7.exercise.refine_data") as mock_refine_data,
            patch("bdi_api.s7.exercise.insert_data_to_postgres") as mock_insert_data,
        ):
            mock_create_table.return_value = None
            mock_refine_data.return_value = [{"icao": "ABC123", "lat": 40.0, "lon": -73.0}]
            mock_insert_data.return_value = None

            response = client.post("/api/s7/aircraft/prepare")

            assert response.status_code == 200
            assert response.json() == "Aircraft data successfully inserted into PostgreSQL."

            mock_create_table.assert_called_once()
            mock_refine_data.assert_called_once()
            mock_insert_data.assert_called_once_with(
                [{"icao": "ABC123", "lat": 40.0, "lon": -73.0}]
        )

    def test_prepare_fails_with_exception(self, client: TestClient):
        with (
            patch("bdi_api.s7.exercise.refine_data", side_effect=Exception("Unexpected error")) as mock_refine_data,
        ):
            response = client.post("/api/s7/aircraft/prepare")

            assert response.status_code == 500
            assert "Processing Error: Unexpected error" in response.json()["detail"]

            mock_refine_data.assert_called_once()

    def test_refine_data_success(self, client: TestClient):
        sample_files = ["raw/day=20231101/test1.json", "raw/day=20231101/test2.json"]
        processed_data = [
            {"icao": "ABC123", "lat": 40.0, "lon": -73.0},
            {"icao": "DEF456", "lat": 41.0, "lon": -74.0}
        ]

        with patch("bdi_api.s7.exercise.list_s3_files") as mock_list_s3_files, \
                patch("bdi_api.s7.exercise.process_s3_files") as mock_process_s3_files:

            mock_list_s3_files.return_value = sample_files
            mock_process_s3_files.return_value = processed_data

            result = refine_data()

            args, _ = mock_list_s3_files.call_args
            assert args[0]
            assert isinstance(args[0], str)
            assert args[1] == "raw/day=20231101/"

            assert result == processed_data

    def test_refine_data_no_files(self, client:TestClient):
        with patch("bdi_api.s7.exercise.list_s3_files") as mock_list_s3_files:
            mock_list_s3_files.return_value = []

            result = refine_data()
            args, _ = mock_list_s3_files.call_args
            assert args[0]
            assert isinstance(args[0], str)
            assert args[1] == "raw/day=20231101/"
            assert result == []


    def test_refine_data_s3_error(self, client:TestClient):
        with patch("bdi_api.s7.exercise.list_s3_files", side_effect=NoCredentialsError):
            with pytest.raises(HTTPException) as exc_info:
                refine_data()

            assert exc_info.value.status_code == 500
            assert "S3 Error" in str(exc_info.value.detail)

    def test_refine_data_generic_error(self, client:TestClient):
        with patch("bdi_api.s7.exercise.list_s3_files", side_effect=Exception("Unexpected Error")):
            with pytest.raises(HTTPException) as exc_info:
                refine_data()

            assert exc_info.value.status_code == 500
            assert "Processing Error" in str(exc_info.value.detail)


    def test_list_aircraft(self, client: TestClient):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        sample_data = [
            ("ABC123", "N12345", "A320"),
            ("XYZ789", "N54321", "B737"),
        ]

        mock_cursor.fetchall.return_value = sample_data

        with (patch("bdi_api.s7.exercise.get_db_connection", return_value=mock_conn)):
            response = client.get("/api/s7/aircraft/", params={"num_results": 2, "page": 1})

            assert response.status_code == 200
            assert response.json() == [
                {"icao": "ABC123", "registration": "N12345", "type": "A320"},
                {"icao": "XYZ789", "registration": "N54321", "type": "B737"},
            ]

            expected_query = dedent("""
                SELECT icao, registration, type
                FROM aircraft_data
                ORDER BY icao ASC
                LIMIT %s OFFSET %s;
            """).strip()

            actual_query = dedent(mock_cursor.execute.call_args[0][0]).strip()
            assert actual_query == expected_query
            mock_cursor.fetchall.assert_called_once()

    def test_get_aircraft_position(self, client: TestClient):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        sample_data = [
            (datetime(2025, 3, 19, 12, 0, 0, tzinfo=timezone.utc), 40.7128, -74.0060),
            (datetime(2025, 3, 19, 12, 5, 0, tzinfo=timezone.utc), 40.7130, -74.0058),
        ]
        mock_cursor.fetchall.return_value = sample_data

        with patch("bdi_api.s7.exercise.get_db_connection", return_value=mock_conn):
            response = client.get("/api/s7/aircraft/ABC123/positions", params={"num_results": 2, "page": 0})

            assert response.status_code == 200

            expected_response = [
                {"timestamp": row[0].timestamp(), "lat": row[1], "lon": row[2]}
                for row in sample_data
            ]

            assert response.json() == expected_response

            expected_query = dedent("""
                SELECT timestamp, lat, lon
                FROM aircraft_data
                WHERE icao = %s
                ORDER BY timestamp ASC
                LIMIT %s OFFSET %s;
            """).strip()

            actual_query = dedent(mock_cursor.execute.call_args[0][0]).strip()
            assert actual_query == expected_query

            mock_cursor.fetchall.assert_called_once()

    def test_get_aircraft_statistics(self, client: TestClient):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        sample_data = [
            (35000, 450, 1),
            (34000, 430, 0),
        ]
        mock_cursor.fetchall.return_value = sample_data

        with patch("bdi_api.s7.exercise.get_db_connection", return_value=mock_conn):
            response = client.get("/api/s7/aircraft/ABC123/stats")

            assert response.status_code == 200

            expected_response = response.json()
            assert isinstance(expected_response, list)

            expected_query = dedent("""
                SELECT max_altitude_baro, max_ground_speed, had_emergency
                FROM aircraft_data
                WHERE icao = %s;
            """).strip()

            actual_query = dedent(mock_cursor.execute.call_args[0][0]).strip()
            assert actual_query == expected_query

            mock_cursor.fetchall.assert_called_once()

    def test_get_json_from_s3(self, client: TestClient) -> None:
        with patch("bdi_api.s7.s7_helper.s3_client.get_object") as mock_get_object:
            mock_get_object.return_value = {"Body": io.BytesIO(b'{"aircraft": [{"icao": "abc123"}]}')}
            from bdi_api.s7.s7_helper import get_json_from_s3

            result = get_json_from_s3("test-bucket", "file1.json")
            assert result == {"aircraft": [{"icao": "abc123"}]}

    def test_list_s3_files(self, client: TestClient) -> None:
        with patch("bdi_api.s7.s7_helper.s3_client.list_objects_v2") as mock_list_objects:
            mock_list_objects.return_value = {"Contents": [{"Key": "file1.json"}, {"Key": "file2.json"}]}
            from bdi_api.s7.s7_helper import list_s3_files

            result = list_s3_files("test-bucket", "test-prefix/")
            assert result == ["file1.json", "file2.json"]

    def test_process_batch_s7(self, client: TestClient):
        aircraft_timestamp = 1742577600  # Example timestamp

        batch = [
            {"hex": "ABC123", "r": "REG123", "t": "A320", "lat": 40.7128, "lon": -74.0060,
                    "alt_baro": 35000, "gs": 450, "emergency": "None"},
            {"hex": "XYZ789", "r": "REG789", "t": "B737", "lat": 41.1234, "lon": -73.9876,
                    "alt_baro": 34000, "gs": 430, "emergency": "low"},
            {"hex": "DEF456", "r": "REG456", "t": "B757", "lon": -72.9876,
                    "alt_baro": 33000, "gs": 420, "emergency": "None"},
            {"hex": "GND001", "r": "REGGND", "t": "C172", "lat": 38.9876, "lon": -71.8765,
                    "alt_baro": "ground", "gs": 55, "emergency": None},
            {"hex": "TWR001", "r": "TWR", "t": "ATC", "lat": 37.9876, "lon": -70.8765,
                    "alt_baro": 5000, "gs": 200, "emergency": "None"},
        ]

        result = process_batch_s7(batch, aircraft_timestamp)

        assert isinstance(result, list), "Output should be a list"
        assert all(isinstance(item, dict) for item in result), "Each item in the list should be a dictionary"
        assert len(result) > 0, "There should be at least one processed aircraft record"

    def test_process_s3_files(self, client: TestClient):
        s3_bucket = "test-bucket"
        file_keys = ["file1.json", "file2.json"]

        mock_file_data = {"aircraft": [{"icao": "ABC123", "lat": 40.7128, "lon": -74.0060,
                                        "max_altitude_baro": 35000, "max_ground_speed": 450, "had_emergency": False}]}

        mock_processed_data = [
            {
                "icao": "ABC123",
                "registration": "N12345",
                "type": "A320",
                "lat": 40.7128,
                "lon": -74.0060,
                "max_altitude_baro": 35000,
                "max_ground_speed": 450,
                "had_emergency": False,
            }
        ]

        with patch("bdi_api.s7.s7_helper.get_json_from_s3", return_value=mock_file_data), \
                patch("bdi_api.s7.s7_helper.process_aircraft_data", return_value=mock_processed_data):

            result = process_s3_files(s3_bucket, file_keys)

            assert isinstance(result, list), "Output should be a list"
            assert all(isinstance(item, dict) for item in result), "Each item in the list should be a dictionary"
            assert len(result) > 0, "Function should return at least one record"




