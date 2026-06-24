import pytest
import pandas as pd

from src.transform.transformer import WeatherTransformer


@pytest.fixture
def transformer():

    return WeatherTransformer()


@pytest.fixture
def sample_weather_data():

    return {
        "daily": {
            "time": ["2026-06-16", "2026-06-17"],
            "temperature_2m_max": [30.5, 28.7],
            "temperature_2m_min": [22.4, 21.8],
            "precipitation_sum": [4.1, 0.9],
            "wind_speed_10m_max": [9.8, 7.3],
        }
    }


def test_create_dataframe(transformer, sample_weather_data):

    df = transformer.create_dataframe(sample_weather_data)

    assert isinstance(df, pd.DataFrame)

    assert len(df) == 2


def test_create_dataframe_missing_daily(transformer):

    with pytest.raises(ValueError, match="missing 'daily' section"):
        transformer.create_dataframe({})


def test_clean_column_names(transformer, sample_weather_data):

    df = transformer.create_dataframe(sample_weather_data)

    df = transformer.clean_column_names(df)

    assert "date" in df.columns

    assert "time" not in df.columns


def test_convert_data_types(transformer, sample_weather_data):

    df = transformer.create_dataframe(sample_weather_data)

    df = transformer.clean_column_names(df)

    df = transformer.convert_data_types(df)

    assert pd.api.types.is_datetime64_any_dtype(df["date"])

    assert pd.api.types.is_numeric_dtype(df["temperature_2m_max"])


def test_create_derived_fields(transformer, sample_weather_data):

    df = transformer.create_dataframe(sample_weather_data)

    df = transformer.clean_column_names(df)

    df = transformer.convert_data_types(df)

    df = transformer.create_derived_fields(df)

    assert "temp_range" in df.columns

    assert "date_key" in df.columns

    assert "load_timestamp" in df.columns

    assert "location_name" in df.columns


def test_split_star_schema(transformer, sample_weather_data):

    df = transformer.create_dataframe(sample_weather_data)

    df = transformer.clean_column_names(df)

    df = transformer.convert_data_types(df)

    df = transformer.create_derived_fields(df)

    result = transformer.split_star_schema(df)

    assert "dim_date" in result

    assert "dim_location" in result

    assert "fact_weather" in result

    assert len(result["dim_date"]) == 2

    assert len(result["dim_location"]) == 1

    assert len(result["fact_weather"]) == 2


def test_transform_returns_star_schema(transformer, sample_weather_data):

    result = transformer.transform(sample_weather_data)

    assert isinstance(result, dict)

    assert "dim_date" in result

    assert "dim_location" in result

    assert "fact_weather" in result
