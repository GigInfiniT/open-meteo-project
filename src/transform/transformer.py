import pandas as pd
from typing import Any

from src.utils.logger import logger
from src.utils.config import config
from src.validation.validator import WeatherValidator


class WeatherTransformer:
    """
    Transforms raw weather API data into a clean
    analytical dataset ready for loading.

    Responsibilities:
        - Convert API response to DataFrame
        - Clean column names
        - Convert data types
        - Create derived fields
    """

    def create_dataframe(self, weather_data: dict[str, Any]) -> pd.DataFrame:
        """
        Convert the weather data section of the API
        response into a pandas DataFrame.
        """
        logger.info("Creating DataFrame from API response")

        WeatherValidator.validate_api_structure(weather_data)

        return pd.DataFrame(weather_data["daily"])

    def clean_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names and rename
        API-specific fields.
        """

        df.columns = df.columns.str.strip().str.lower()

        df = df.rename(columns={"time": "date"})

        return df

    def convert_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert columns to appropriate data types.
        """

        df["date"] = pd.to_datetime(df["date"], errors="coerce")

        numeric_columns = [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "wind_speed_10m_max",
        ]

        for column in numeric_columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

        return df

    def create_derived_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create analytical fields used for reporting.
        """

        # Temperature range

        df["temp_range"] = df["temperature_2m_max"] - df["temperature_2m_min"]

        # Location attributes

        df["location_name"] = config.LOCATION_NAME

        df["latitude"] = config.LATITUDE

        df["longitude"] = config.LONGITUDE

        # Date dimension key

        df["date_key"] = df["date"].dt.strftime("%Y%m%d").astype(int)

        # Date attributes

        df["year"] = df["date"].dt.year

        df["month"] = df["date"].dt.month

        df["day"] = df["date"].dt.day

        df["day_of_week"] = df["date"].dt.day_name()

        # ETL audit timestamp

        df["load_timestamp"] = pd.Timestamp.utcnow()

        return df

    def split_star_schema(self, df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        """
        Split transformed data into star schema
        structures for database loading.
        """

        logger.info("Preparing fact and dimension tables")

        # Date dimension

        dim_date = (
            df[["date_key", "date", "year", "month", "day", "day_of_week"]]
            .drop_duplicates(subset=["date_key"])
            .copy()
        )

        dim_date["date"] = dim_date["date"].dt.date

        # Location dimension

        dim_location = (
            df[["location_name", "latitude", "longitude"]].drop_duplicates().copy()
        )

        # Weather fact table

        fact_weather = df[
            [
                "date_key",
                "location_name",
                "temperature_2m_max",
                "temperature_2m_min",
                "precipitation_sum",
                "wind_speed_10m_max",
                "temp_range",
                "load_timestamp",
            ]
        ].copy()

        return {
            "dim_date": dim_date,
            "dim_location": dim_location,
            "fact_weather": fact_weather,
        }

    def transform(self, weather_data: dict[str, Any]) -> dict[str, pd.DataFrame]:
        """
        Execute the complete transformation workflow.
        """

        logger.info("Starting transformation process")

        df = self.create_dataframe(weather_data)

        logger.info(f"Raw API record count: {len(df)}")

        WeatherValidator.validate_required_columns(df)

        df = self.clean_column_names(df)

        df = self.convert_data_types(df)

        df = WeatherValidator.handle_missing_values(df)

        df = WeatherValidator.remove_duplicates(df)

        df = WeatherValidator.validate_weather_measurements(df)

        df = self.create_derived_fields(df)

        logger.info(f"Final record count: {len(df)}")

        star_schema_data = self.split_star_schema(df)

        logger.info(f"dim_date rows: {len(star_schema_data['dim_date'])}")

        logger.info(f"dim_location rows: {len(star_schema_data['dim_location'])}")

        logger.info(f"fact_weather rows: {len(star_schema_data['fact_weather'])}")

        logger.info("Transformation completed successfully")

        return star_schema_data
