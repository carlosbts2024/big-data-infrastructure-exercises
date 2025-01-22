from fastapi.testclient import TestClient
from unittest.mock import patch

class TestS1Student:
    """
    As you code it's always important to ensure that your code reflects
    the business requisites you have.
    The optimal way to do so is via tests.
    Use this class to create functions to test your application.

    For more information on the library used, search `pytest` in your preferred search engine.
    """

    def test_first(self, client: TestClient) -> None:
        # Implement tests if you want
        with client as client:
            response = client.post("/api/s1/aircraft/download?file_limit=1")
            assert True

    def test_download_data_internal_server_error(self, client: TestClient) -> None:

        with patch("bdi_api.s1.exercise.requests.get") as mock_get:
            mock_get.side_effect = Exception("Mocked exception for testing")
            response = client.post("/api/s1/aircraft/download?file_limit=1")

            assert response.status_code == 500
            assert response.json() == {
                "detail": "Failed to fetch or download files: Mocked exception for testing"
            }

    def test_download_invalid_file_limit(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s1/aircraft/download?file_limit=-1")
            assert response.status_code == 422, "file_limit must be greater than 0"

    def test_no_files_to_download(self, client: TestClient) -> None:
        with patch("bdi_api.s1.exercise.BeautifulSoup") as mock_soup:
            mock_soup.return_value.find_all.return_value = []

            response = client.post("/api/s1/aircraft/download?file_limit=1")
            assert response.status_code == 200

    def test_list_aircraft_file_not_found(self, client: TestClient) -> None:
        with patch("bdi_api.s1.exercise.os.path.exists") as mock_exists:
            mock_exists.return_value = False
            response = client.get("/api/s1/aircraft/")

            assert response.status_code == 404
            assert response.json() == {
                "detail": "Prepared data file not found."
            }

    def test_list_aircraft_internal_server_error(self, client: TestClient) -> None:
        with patch("bdi_api.s1.exercise.pd.read_json") as mock_read_json:
            mock_read_json.side_effect = Exception("Mocked exception for testing")
            response = client.get("/api/s1/aircraft/")

            assert response.status_code == 500
            assert response.json() == {
                "detail": "Failed to list aircraft: Mocked exception for testing"
            }
    def test_get_aircraft_positions_internal_server_error(self, client: TestClient) -> None:
        with patch("bdi_api.s1.exercise.pd.read_json") as mock_read_json:
            mock_read_json.side_effect = Exception("Mocked exception for testing")
            response = client.get("/api/s1/aircraft/000000/positions")

            assert response.status_code == 500
            assert response.json() == {
                "detail": "Failed to retrieve aircraft positions: Mocked exception for testing"
            }

    def test_get_aircraft_statistics_file_not_found(self, client: TestClient) -> None:
        with patch("bdi_api.s1.exercise.os.path.exists") as mock_exists:
            mock_exists.return_value = False
            response = client.get("/api/s1/aircraft/000000/stats")

            assert response.status_code == 404
            assert response.json() == {
                "detail": "Prepared data file not found."
            }


class TestItCanBeEvaluated:
    """
    Those tests are just to be sure I can evaluate your exercise.
    Don't modify anything from here!

    Make sure all those tests pass with `poetry run pytest` or it will be a 0!
    """

    def test_download(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s1/aircraft/download?file_limit=1")
            assert not response.is_error, "Error at the download endpoint"

    def test_prepare(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s1/aircraft/prepare")
            assert not response.is_error, "Error at the prepare endpoint"

    def test_aircraft(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s1/aircraft")
            assert not response.is_error, "Error at the aircraft endpoint"
            r = response.json()
            assert isinstance(r, list), "Result is not a list"
            assert len(r) > 0, "Result is empty"
            for field in ["icao", "registration", "type"]:
                assert field in r[0], f"Missing '{field}' field."

    def test_positions(self, client: TestClient) -> None:
        icao = "06a0af"
        with client as client:
            response = client.get(f"/api/s1/aircraft/{icao}/positions")
            assert not response.is_error, "Error at the positions endpoint"
            r = response.json()
            assert isinstance(r, list), "Result is not a list"
            assert len(r) > 0, "Result is empty"
            for field in ["timestamp", "lat", "lon"]:
                assert field in r[0], f"Missing '{field}' field."

    def test_stats(self, client: TestClient) -> None:
        icao = "06a0af"
        with client as client:
            response = client.get(f"/api/s1/aircraft/{icao}/stats")
            assert not response.is_error, "Error at the positions endpoint"
            r = response.json()
            for field in ["max_altitude_baro", "max_ground_speed", "had_emergency"]:
                assert field in r, f"Missing '{field}' field."
