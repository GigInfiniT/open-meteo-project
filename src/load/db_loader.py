import pandas as pd

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from src.utils.logger import logger
from src.utils.config import config


class WeatherLoader:
    """
    Loads transformed weather data into PostgreSQL.

    Features:
        - Batch inserts
        - Idempotent dimension loads
        - Fact table upserts
        - Surrogate key resolution
        - Transaction management
        - Error handling and logging
    """

    def __init__(self) -> None:

        self.engine = create_engine(
            f"postgresql+psycopg2://"
            f"{config.DB_USER}:"
            f"{config.DB_PASSWORD}@"
            f"{config.DB_HOST}:"
            f"{config.DB_PORT}/"
            f"{config.DB_NAME}"
        )

    def load_dim_date(self, dim_date: pd.DataFrame) -> None:
        """
        Load date dimension table.
        Existing dates are ignored.
        """

        if dim_date.empty:
            logger.warning("dim_date is empty. Nothing to load.")

            return

        logger.info("Loading dim_date table...")

        query = text(
            """
            INSERT INTO dim_date (
                date_key,
                date,
                year,
                month,
                day,
                day_of_week
            )
            VALUES (
                :date_key,
                :date,
                :year,
                :month,
                :day,
                :day_of_week
            )
            ON CONFLICT (date)
            DO NOTHING
            """
        )

        try:
            with self.engine.begin() as conn:
                conn.execute(query, dim_date.to_dict(orient="records"))

            logger.info(f"Processed {len(dim_date)} dim_date rows")

        except SQLAlchemyError as e:
            logger.error(f"Failed loading dim_date: {e}")

            raise

    def load_dim_location(self, dim_location: pd.DataFrame) -> None:
        """
        Load location dimension table.
        Existing locations are ignored.
        """

        if dim_location.empty:
            logger.warning("dim_location is empty. Nothing to load.")

            return

        logger.info("Loading dim_location table")

        query = text(
            """
            INSERT INTO dim_location (
                location_name,
                latitude,
                longitude
            )
            VALUES (
                :location_name,
                :latitude,
                :longitude
            )
            ON CONFLICT (location_name)
            DO NOTHING
            """
        )

        try:
            with self.engine.begin() as conn:
                conn.execute(query, dim_location.to_dict(orient="records"))

            logger.info(f"Processed {len(dim_location)} dim_location rows")

        except SQLAlchemyError as e:
            logger.error(f"Failed loading dim_location: {e}")

            raise

    def get_location_key_mapping(self) -> dict[str, int]:
        """
        Retrieve all location surrogate keys.
        """

        query = text(
            """
            SELECT
                location_name,
                location_key
            FROM dim_location
            """
        )

        try:
            with self.engine.connect() as conn:
                result = conn.execute(query)

                return {row.location_name: row.location_key for row in result}

        except SQLAlchemyError as e:
            logger.error(f"Failed retrieving location mapping: {e}")

            raise

    def prepare_fact_table(self, fact_weather: pd.DataFrame) -> pd.DataFrame:
        """
        Replace location_name with location_key.
        """

        if fact_weather.empty:
            logger.warning("fact_weather is empty.")

            return fact_weather

        logger.info("Resolving location surrogate keys")

        mapping = self.get_location_key_mapping()

        fact_weather = fact_weather.copy()

        fact_weather["location_key"] = fact_weather["location_name"].map(mapping)

        if fact_weather["location_key"].isnull().any():
            missing_locations = (
                fact_weather[fact_weather["location_key"].isnull()]["location_name"]
                .unique()
                .tolist()
            )

            raise ValueError(f"Missing location keys for: {missing_locations}")

        fact_weather["location_key"] = fact_weather["location_key"].astype(int)

        fact_weather = fact_weather.drop(columns=["location_name"])

        return fact_weather

    def load_fact_weather(self, fact_weather: pd.DataFrame) -> None:
        """
        Load weather fact table.

        Existing records are updated when
        the same location/date combination
        already exists.
        """

        if fact_weather.empty:
            logger.warning("fact_weather is empty. Nothing to load.")

            return

        logger.info("Loading fact_weather table")

        query = text(
            """
            INSERT INTO fact_weather (
                date_key,
                location_key,
                temperature_2m_max,
                temperature_2m_min,
                precipitation_sum,
                wind_speed_10m_max,
                temp_range,
                load_timestamp
            )
            VALUES (
                :date_key,
                :location_key,
                :temperature_2m_max,
                :temperature_2m_min,
                :precipitation_sum,
                :wind_speed_10m_max,
                :temp_range,
                :load_timestamp
            )

            ON CONFLICT ON CONSTRAINT
                uq_weather_day_location

            DO UPDATE SET
                temperature_2m_max =
                    EXCLUDED.temperature_2m_max,

                temperature_2m_min =
                    EXCLUDED.temperature_2m_min,

                precipitation_sum =
                    EXCLUDED.precipitation_sum,

                wind_speed_10m_max =
                    EXCLUDED.wind_speed_10m_max,

                temp_range =
                    EXCLUDED.temp_range,

                load_timestamp =
                    EXCLUDED.load_timestamp
            """
        )

        try:
            with self.engine.begin() as conn:
                conn.execute(query, fact_weather.to_dict(orient="records"))

            logger.info(f"Processed {len(fact_weather)} fact_weather rows")

        except SQLAlchemyError as e:
            logger.error(f"Failed loading fact_weather: {e}")

            raise

    def load(self, star_schema_data: dict[str, pd.DataFrame]) -> None:
        """
        Execute complete database load.
        """

        try:
            logger.info("Starting database load")

            self.load_dim_date(star_schema_data["dim_date"])

            self.load_dim_location(star_schema_data["dim_location"])

            fact_weather = self.prepare_fact_table(star_schema_data["fact_weather"])

            self.load_fact_weather(fact_weather)

            logger.info("Database load completed successfully")

        except Exception as exc:
            logger.critical(f"Database load failed: {exc}")

            raise
