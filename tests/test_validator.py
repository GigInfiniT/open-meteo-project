import pytest
import pandas as pd

from src.validation.validator import WeatherValidator


def test_validate_api_structure_success():

    WeatherValidator.validate_api_structure({"daily": {}})


def test_validate_api_structure_failure():

    with pytest.raises(ValueError, match="missing 'daily' section"):
        WeatherValidator.validate_api_structure({})


def test_validate_required_columns_success():

    df = pd.DataFrame(
        columns=[
            "time",
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "wind_speed_10m_max",
        ]
    )

    WeatherValidator.validate_required_columns(df)


def test_validate_required_columns_failure():

    df = pd.DataFrame(columns=["time", "temperature_2m_max"])

    with pytest.raises(ValueError, match="Missing required columns"):
        WeatherValidator.validate_required_columns(df)


def test_handle_missing_values():

    df = pd.DataFrame(
        {
            "date": ["2026-06-16", None],
            "temperature_2m_max": [30, 31],
            "temperature_2m_min": [20, 21],
            "precipitation_sum": [1, 2],
            "wind_speed_10m_max": [5, None],
        }
    )

    result = WeatherValidator.handle_missing_values(df)

    assert len(result) == 1


def test_remove_duplicates():

    df = pd.DataFrame(
        {
            "date": ["2026-06-16", "2026-06-16"],
            "temperature_2m_max": [30, 30],
            "temperature_2m_min": [20, 20],
            "precipitation_sum": [1, 1],
            "wind_speed_10m_max": [5, 5],
        }
    )

    result = WeatherValidator.remove_duplicates(df)

    assert len(result) == 1


def test_validate_weather_measurements():

    df = pd.DataFrame(
        {
            "temperature_2m_max": [30, 20],
            "temperature_2m_min": [20, 25],
            "precipitation_sum": [1, 1],
            "wind_speed_10m_max": [5, 5],
        }
    )

    result = WeatherValidator.validate_weather_measurements(df)

    assert len(result) == 1


def test_validate_negative_precipitation():

    df = pd.DataFrame(
        {
            "temperature_2m_max": [30],
            "temperature_2m_min": [20],
            "precipitation_sum": [-1],
            "wind_speed_10m_max": [5],
        }
    )

    result = WeatherValidator.validate_weather_measurements(df)

    assert result.empty


def test_validate_negative_wind_speed():

    df = pd.DataFrame(
        {
            "temperature_2m_max": [30],
            "temperature_2m_min": [20],
            "precipitation_sum": [1],
            "wind_speed_10m_max": [-5],
        }
    )

    result = WeatherValidator.validate_weather_measurements(df)

    assert result.empty
