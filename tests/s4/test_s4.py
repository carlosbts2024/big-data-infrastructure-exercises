import io
from unittest.mock import patch

from botocore.exceptions import NoCredentialsError
from fastapi.testclient import TestClient


class TestS4Student:
    def test_first(self, client: TestClient) -> None:
        with client as client:
            client.post("/api/s4/aircraft/download?file_limit=1")
            assert True

    def test_download_success(self, client: TestClient) -> None:
        with (
            patch("bdi_api.s4.exercise.delete_files_in_s3_folder") as mock_delete_files,
            patch("bdi_api.s4.exercise.requests.get") as mock_get,
            patch("bdi_api.s4.s4_helper.s3_client.upload_fileobj") as mock_upload,
        ):
            mock_delete_files.return_value = None
            mock_get.return_value.status_code = 200
            mock_get.return_value.text = '<tr class="file"><a href="file1.json.gz"></a></tr>'
            mock_get.return_value.content = b"file content"
            mock_upload.return_value = None

            response = client.post("/api/s4/aircraft/download?file_limit=1")

            assert response.status_code == 200
            assert response.json() == "OK"

    def test_download_no_files(self, client: TestClient) -> None:
        with (
            patch("bdi_api.s4.exercise.delete_files_in_s3_folder") as mock_delete_files,
            patch("bdi_api.s4.exercise.requests.get") as mock_get,
            patch("bdi_api.s4.exercise.s3_client.upload_fileobj") as mock_upload,
        ):
            mock_delete_files.return_value = None
            mock_get.return_value.status_code = 200
            mock_get.return_value.text = ""
            mock_upload.return_value = None

            response = client.post("/api/s4/aircraft/download?file_limit=1")

            assert response.status_code == 200
            assert response.json() == "No files found to download."

    def test_download_data_internal_server_error(self, client: TestClient) -> None:
        with (
            patch("bdi_api.s4.exercise.delete_files_in_s3_folder") as mock_delete_files,
            patch("bdi_api.s4.exercise.requests.get") as mock_get,
            patch("bdi_api.s4.exercise.s3_client.upload_fileobj") as mock_upload,
        ):
            mock_delete_files.return_value = None
            mock_get.side_effect = Exception("Mocked exception for testing")
            mock_upload.return_value = None

            response = client.post("/api/s4/aircraft/download?file_limit=1")

            assert response.status_code == 500
            assert response.json() == {"detail": "Failed to fetch or download files: Mocked exception for testing"}

    def test_download_invalid_file_limit(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s4/aircraft/download?file_limit=-1")
            assert response.status_code == 422, "file_limit must be greater than 0"

    def test_no_files_to_download(self, client: TestClient) -> None:
        with (
            patch("bdi_api.s4.exercise.delete_files_in_s3_folder") as mock_delete_files,
            patch("bdi_api.s4.exercise.requests.get") as mock_get,
            patch("bdi_api.s4.exercise.BeautifulSoup") as mock_soup,
        ):
            mock_delete_files.return_value = None

            mock_get.return_value.status_code = 200
            mock_get.return_value.text = ""

            mock_soup.return_value.find_all.return_value = []

            response = client.post("/api/s4/aircraft/download?file_limit=1")

            assert response.status_code == 200
            assert response.json() == "No files found to download."

    def test_download_data_forbidden_error(self, client: TestClient) -> None:
        with (
            patch("bdi_api.s4.exercise.delete_files_in_s3_folder") as mock_delete_files,
            patch("bdi_api.s4.exercise.requests.get") as mock_get,
        ):
            mock_delete_files.return_value = None

            mock_get.return_value.status_code = 403
            mock_get.return_value.text = "Forbidden"

            response = client.post("/api/s4/aircraft/download?file_limit=1")

            assert response.status_code == 200
            assert response.json() == "No files found to download."

    def test_prepare_no_valid_aircraft_data(self, client: TestClient) -> None:
        with (
            patch("bdi_api.s4.exercise.list_s3_files") as mock_list_s3_files,
            patch("bdi_api.s4.exercise.process_s3_files") as mock_process_s3_files,
        ):
            mock_list_s3_files.return_value = ["raw/day=20231101/test.json"]
            mock_process_s3_files.return_value = []

            response = client.post("/api/s4/aircraft/prepare")

            assert response.status_code == 200
            assert response.json() == "No valid aircraft data found for preparation."

    def test_prepare_no_files_found(self, client: TestClient) -> None:
        with patch("bdi_api.s4.exercise.list_s3_files") as mock_list_s3_files:
            mock_list_s3_files.return_value = []

            response = client.post("/api/s4/aircraft/prepare")

            assert response.status_code == 200
            assert response.json() == "No files found to process."

    def test_prepare_success(self, client: TestClient) -> None:
        with (
            patch("bdi_api.s4.exercise.list_s3_files") as mock_list_s3_files,
            patch("bdi_api.s4.exercise.process_s3_files") as mock_process_s3_files,
            patch("bdi_api.s4.exercise.upload_to_s3") as mock_upload_to_s3,
            patch("bdi_api.s4.s4_helper.process_aircraft_data") as mock_process_aircraft_data,
        ):
            mock_list_s3_files.return_value = ["file1.json"]
            mock_process_s3_files.return_value = [{"icao": "abc123"}]
            mock_process_aircraft_data.return_value = [{"icao": "abc123"}]
            mock_upload_to_s3.return_value = None

            response = client.post("/api/s4/aircraft/prepare")

            assert response.status_code == 200
            assert response.json() == "Aircraft data prepared and uploaded to S3."

    def test_prepare_no_files(self, client: TestClient) -> None:
        with patch("bdi_api.s4.exercise.list_s3_files") as mock_list_s3_files:
            mock_list_s3_files.return_value = []

            response = client.post("/api/s4/aircraft/prepare")

            assert response.status_code == 200
            assert response.json() == "No files found to process."

    def test_prepare_upload_error(self, client: TestClient) -> None:
        with (
            patch("bdi_api.s4.exercise.list_s3_files") as mock_list_s3_files,
            patch("bdi_api.s4.s4_helper.get_json_from_s3") as mock_get_json_from_s3,
            patch("bdi_api.s4.s4_helper.process_aircraft_data") as mock_process_aircraft_data,
            patch("bdi_api.s4.exercise.upload_to_s3") as mock_upload_to_s3,
        ):
            mock_list_s3_files.return_value = ["file1.json"]
            mock_get_json_from_s3.return_value = {"aircraft": [{"icao": "abc123"}]}
            mock_process_aircraft_data.return_value = [{"icao": "abc123"}]
            mock_upload_to_s3.side_effect = NoCredentialsError()

            response = client.post("/api/s4/aircraft/prepare")

            assert response.status_code == 500
            assert "S3 Error" in response.json()["detail"]

    def test_process_s3_files(self, client: TestClient) -> None:
        with (
            patch("bdi_api.s4.exercise.list_s3_files") as mock_list_s3_files,
            patch("bdi_api.s4.s4_helper.get_json_from_s3") as mock_get_json_from_s3,
            patch("bdi_api.s4.s4_helper.process_aircraft_data") as mock_process_aircraft_data,
            patch("bdi_api.s4.exercise.upload_to_s3") as mock_upload_to_s3,
        ):
            mock_list_s3_files.return_value = ["file1.json"]
            mock_get_json_from_s3.return_value = {"aircraft": [{"icao": "abc123"}]}
            mock_process_aircraft_data.return_value = [{"icao": "abc123"}]
            mock_upload_to_s3.return_value = None

            response = client.post("/api/s4/aircraft/prepare")

            assert response.status_code == 200
            assert response.json() == "Aircraft data prepared and uploaded to S3."

    def test_list_s3_files(self, client: TestClient) -> None:
        with patch("bdi_api.s4.s4_helper.s3_client.list_objects_v2") as mock_list_objects:
            mock_list_objects.return_value = {"Contents": [{"Key": "file1.json"}, {"Key": "file2.json"}]}
            from bdi_api.s4.s4_helper import list_s3_files

            result = list_s3_files("test-bucket", "test-prefix/")
            assert result == ["file1.json", "file2.json"]

    def test_get_json_from_s3(self, client: TestClient) -> None:
        with patch("bdi_api.s4.s4_helper.s3_client.get_object") as mock_get_object:
            mock_get_object.return_value = {"Body": io.BytesIO(b'{"aircraft": [{"icao": "abc123"}]}')}
            from bdi_api.s4.s4_helper import get_json_from_s3

            result = get_json_from_s3("test-bucket", "file1.json")
            assert result == {"aircraft": [{"icao": "abc123"}]}

    def test_upload_to_s3(self, client: TestClient) -> None:
        with patch("bdi_api.s4.s4_helper.s3_client.upload_fileobj") as mock_upload_fileobj:
            from bdi_api.s4.s4_helper import upload_to_s3

            upload_to_s3([{"icao": "abc123"}], "test-bucket", "output-prefix/")
            mock_upload_fileobj.assert_called_once()

    def test_process_aircraft_data(self, client: TestClient) -> None:
        from bdi_api.s4.s4_helper import process_aircraft_data

        data = {
            "aircraft": [
                {"hex": "abc123", "lat": 50.0, "lon": 30.0, "alt_baro": 10000, "gs": 500},
                {"hex": "def456", "lat": 51.0, "lon": 31.0, "alt_baro": -500, "gs": 450},
                {"hex": "ghi789", "r": "TWR"},
            ],
        }
        result = process_aircraft_data(data)
        assert len(result) == 2
        assert result[0]["icao"] == "abc123"
        assert result[0]["max_altitude_baro"] == 10000
        assert result[1]["icao"] == "def456"
        assert result[1]["max_altitude_baro"] == 0
