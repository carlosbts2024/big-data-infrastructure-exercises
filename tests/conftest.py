import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unittest.mock import patch

@pytest.fixture(scope="session", autouse=True)
def mock_db_credentials():
    """Mock database credentials for all tests."""
    with patch("bdi_api.s7.exercise.get_db_credentials") as mock_creds:
        mock_creds.return_value.host = "localhost"
        mock_creds.return_value.port = 5432
        mock_creds.return_value.username = "test_user"
        mock_creds.return_value.password = "test_pass"
        mock_creds.return_value.database = "test_db"
        yield

from bdi_api.app import app as real_app

@pytest.fixture(scope="class")
def app() -> FastAPI:
    """In case you want to test only a part"""
    return real_app

@pytest.fixture(scope="class")
def client(app: FastAPI) -> TestClient:
    """We include our router for the examples"""
    yield TestClient(app)