import pandas as pd
import pytest

from unittest.mock import MagicMock

from src.load.db_loader import WeatherLoader


@pytest.fixture
def loader():

    return WeatherLoader()


def test_prepare_fact_table_maps_location_key(loader, monkeypatch):

    monkeypatch.setattr(loader, "get_location_key_mapping", lambda: {"Abuja": 1})

    fact_weather = pd.DataFrame(
        {
            "date_key": [20260616],
            "location_name": ["Abuja"],
            "temperature_2m_max": [30.5],
            "temperature_2m_min": [22.4],
            "precipitation_sum": [4.1],
            "wind_speed_10m_max": [9.8],
            "temp_range": [8.1],
            "load_timestamp": [pd.Timestamp.utcnow()],
        }
    )

    result = loader.prepare_fact_table(fact_weather)

    assert "location_key" in result.columns

    assert "location_name" not in result.columns

    assert result.iloc[0]["location_key"] == 1


def test_prepare_fact_table_missing_location(loader, monkeypatch):

    monkeypatch.setattr(loader, "get_location_key_mapping", lambda: {"Abuja": 1})

    fact_weather = pd.DataFrame({"location_name": ["Lagos"]})

    with pytest.raises(ValueError, match="Missing location keys"):
        loader.prepare_fact_table(fact_weather)


def test_prepare_fact_table_empty_dataframe(loader):

    empty_df = pd.DataFrame()

    result = loader.prepare_fact_table(empty_df)

    assert result.empty


def test_get_location_key_mapping(loader, monkeypatch):

    class FakeResult:
        def __iter__(self):

            return iter(
                [type("Row", (), {"location_name": "Abuja", "location_key": 1})()]
            )

    class FakeConnection:
        def execute(self, query):
            return FakeResult()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    monkeypatch.setattr(loader.engine, "connect", lambda: FakeConnection())

    result = loader.get_location_key_mapping()

    assert result == {"Abuja": 1}


def test_load_orchestration(loader, monkeypatch):

    mock_dim_date = MagicMock()

    mock_dim_location = MagicMock()

    mock_prepare = MagicMock(return_value=pd.DataFrame())

    mock_fact = MagicMock()

    monkeypatch.setattr(loader, "load_dim_date", mock_dim_date)

    monkeypatch.setattr(loader, "load_dim_location", mock_dim_location)

    monkeypatch.setattr(loader, "prepare_fact_table", mock_prepare)

    monkeypatch.setattr(loader, "load_fact_weather", mock_fact)

    data = {
        "dim_date": pd.DataFrame(),
        "dim_location": pd.DataFrame(),
        "fact_weather": pd.DataFrame(),
    }

    loader.load(data)

    mock_dim_date.assert_called_once()

    mock_dim_location.assert_called_once()

    mock_prepare.assert_called_once()

    mock_fact.assert_called_once()


def test_load_dim_date_empty_dataframe(loader):

    empty_df = pd.DataFrame()

    loader.load_dim_date(empty_df)


def test_load_dim_location_empty_dataframe(loader):

    empty_df = pd.DataFrame()

    loader.load_dim_location(empty_df)


def test_load_fact_weather_empty_dataframe(loader):

    empty_df = pd.DataFrame()

    loader.load_fact_weather(empty_df)
