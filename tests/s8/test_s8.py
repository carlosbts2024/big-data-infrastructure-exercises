from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient


class TestS8Student:

    def test_list_aircraft(self, client: TestClient):
        """Test listing aircraft with mocked DB connection."""

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        sample_data = [
            ("ABC123", "N12345", "A320", "Gulfstream", "G650", "John Doe"),
            ("XYZ789", "N54321", "B737", "Boeing", "737-800", "Acme Airlines"),
        ]
        mock_cursor.fetchall.return_value = sample_data

        with (
            patch("bdi_api.s8.exercise.get_db_conn", return_value=mock_conn),
            patch("bdi_api.s8.exercise.put_db_conn", return_value=None),  # <--- ADD THIS
        ):
            response = client.get("/api/s8/aircraft/", params={"num_results": 2, "page": 0})

        assert response.status_code == 200
        result = response.json()
        assert isinstance(result, list)
        assert result[0]["icao"] == "ABC123"

def test_get_aircraft_co2(client: TestClient):
    """Test calculating CO2 for a specific aircraft and day."""

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.fetchone.side_effect = [
        (720,),  # 720 records
        ("A320",),  # aircraft type
        (500,),  # galph value
    ]

    with (
        patch("bdi_api.s8.exercise.get_db_conn", return_value=mock_conn),
        patch("bdi_api.s8.exercise.put_db_conn", return_value=None),  # <--- ADD THIS
    ):
        response = client.get("/api/s8/aircraft/ad4482/co2?day=2023-11-01")

    assert response.status_code == 200
    result = response.json()
    assert result["icao"] == "ad4482"
    assert "hours_flown" in result
    assert "co2" in result






