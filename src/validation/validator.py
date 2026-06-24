import pandas as pd
from typing import Any
from src.utils.logger import logger


class WeatherValidator:
    """
    Handles all data quality validation
    and cleansing operations.
    """

    REQUIRED_COLUMNS: tuple[str, ...] = (
        "time",
        "temperature_2m_max",
        "temperature_2m_min",
        "precipitation_sum",
        "wind_speed_10m_max",
    )

    @staticmethod
    def validate_api_structure(weather_data: dict[str, Any]) -> None:
        """
        Validate that the API response
        contains the expected structure.
        """

        if "daily" not in weather_data:
            raise ValueError("Weather API response missing 'daily' section")

    @classmethod
    def validate_required_columns(cls, df: pd.DataFrame) -> None:
        """
        Validate that all required API columns exist.
        """

        missing_columns = [
            column for column in cls.REQUIRED_COLUMNS if column not in df.columns
        ]

        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

    @staticmethod
    def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove records containing
        null values in critical fields.
        """

        before_count = len(df)

        df = df.dropna(
            subset=[
                "date",
                "temperature_2m_max",
                "temperature_2m_min",
                "precipitation_sum",
                "wind_speed_10m_max",
            ]
        )

        removed_count = before_count - len(df)

        if removed_count > 0:
            logger.warning(f"Removed {removed_count} rows with missing values")

        return df

    @staticmethod
    def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate weather records.
        """

        duplicate_count = df.duplicated(subset=["date"]).sum()

        if duplicate_count > 0:
            logger.warning(f"Found {duplicate_count} duplicate records")

        return df.drop_duplicates(subset=["date"]).reset_index(drop=True)

    @staticmethod
    def validate_weather_measurements(df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply weather business rules.
        """

        before_count = len(df)

        df = df[df["temperature_2m_max"] >= df["temperature_2m_min"]]

        df = df[df["precipitation_sum"] >= 0]

        df = df[df["wind_speed_10m_max"] >= 0]

        removed_count = before_count - len(df)

        if removed_count > 0:
            logger.warning(f"Removed {removed_count} invalid weather records")

        return df
