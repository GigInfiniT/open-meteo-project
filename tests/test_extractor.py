import pytest
import requests

from unittest.mock import Mock

from src.extract.api_extractor import WeatherExtractor


@pytest.fixture
def extractor():

    return WeatherExtractor()


def test_build_params(extractor):

    params = extractor.build_params()

    assert "latitude" in params

    assert "longitude" in params

    assert "daily" in params

    assert params["timezone"] == "auto"


def test_validate_response_success(extractor):

    response = Mock()

    response.status_code = 200

    response.json.return_value = {"daily": {}}

    result = extractor.validate_response(response)

    assert result == {"daily": {}}


def test_validate_response_non_200(extractor):

    response = Mock()

    response.status_code = 500

    with pytest.raises(ValueError, match="API returned status code"):
        extractor.validate_response(response)


def test_validate_response_invalid_json(extractor):

    response = Mock()

    response.status_code = 200

    response.json.side_effect = ValueError()

    with pytest.raises(ValueError, match="not valid JSON"):
        extractor.validate_response(response)


def test_validate_response_empty_json(extractor):

    response = Mock()

    response.status_code = 200

    response.json.return_value = {}

    with pytest.raises(ValueError, match="empty response"):
        extractor.validate_response(response)


def test_validate_response_missing_daily(extractor):

    response = Mock()

    response.status_code = 200

    response.json.return_value = {"latitude": 9.0765}

    with pytest.raises(ValueError, match="'daily' field missing"):
        extractor.validate_response(response)


def test_get_weather_data_success(extractor, monkeypatch):

    mock_response = Mock()

    mock_response.status_code = 200

    mock_response.json.return_value = {"daily": {}}

    mock_response.raise_for_status = Mock()

    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: mock_response)

    result = extractor.get_weather_data()

    assert result == {"daily": {}}


def test_get_weather_data_timeout(extractor, monkeypatch):

    def mock_timeout(*args, **kwargs):
        raise requests.exceptions.Timeout()

    monkeypatch.setattr(requests, "get", mock_timeout)

    with pytest.raises(requests.exceptions.Timeout):
        extractor.get_weather_data()


def test_get_weather_data_connection_error(extractor, monkeypatch):

    def mock_connection_error(*args, **kwargs):
        raise requests.exceptions.ConnectionError()

    monkeypatch.setattr(requests, "get", mock_connection_error)

    with pytest.raises(requests.exceptions.ConnectionError):
        extractor.get_weather_data()


def test_get_weather_data_request_exception(extractor, monkeypatch):

    def mock_request_exception(*args, **kwargs):
        raise requests.exceptions.RequestException()

    monkeypatch.setattr(requests, "get", mock_request_exception)

    with pytest.raises(requests.exceptions.RequestException):
        extractor.get_weather_data()
