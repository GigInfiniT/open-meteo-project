import pandas as pd
from sqlalchemy import text

from src.extract.api_extractor import WeatherExtractor
from src.transform.transformer import WeatherTransformer
from src.load.db_loader import WeatherLoader


def test_end_to_end_pipeline():

    # Extract
    extractor = WeatherExtractor()

    weather_data = extractor.get_weather_data()

    assert weather_data is not None
    assert "daily" in weather_data

    # Transform
    transformer = WeatherTransformer()

    star_schema_data = transformer.transform(weather_data)

    assert "dim_date" in star_schema_data
    assert "dim_location" in star_schema_data
    assert "fact_weather" in star_schema_data

    # Load
    loader = WeatherLoader()

    loader.load(star_schema_data)

    # Verify database contents
    with loader.engine.connect() as conn:
        dim_date_count = conn.execute(text("SELECT COUNT(*) FROM dim_date")).scalar()

        dim_location_count = conn.execute(
            text("SELECT COUNT(*) FROM dim_location")
        ).scalar()

        fact_weather_count = conn.execute(
            text("SELECT COUNT(*) FROM fact_weather")
        ).scalar()

    assert dim_date_count > 0
    assert dim_location_count > 0
    assert fact_weather_count > 0
