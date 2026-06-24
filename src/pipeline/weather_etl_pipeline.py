from typing import Any
import pandas as pd

from src.extract.api_extractor import WeatherExtractor
from src.transform.transformer import WeatherTransformer
from src.load.db_loader import WeatherLoader
from src.utils.logger import logger


class ETLPipeline:
    """
    End-to-end ETL workflow.

    Flow:
    1. Extract data from API
    2. Transform raw data
    3. Load dimension and fact tables
    """

    def __init__(self) -> None:

        self.extractor = WeatherExtractor()

        self.transformer = WeatherTransformer()

        self.loader = WeatherLoader()

    def extract_data(self) -> dict[str, Any]:
        """
        Extract weather data from API.
        """

        logger.info("Starting extraction phase")

        weather_data = self.extractor.get_weather_data()

        logger.info("Extraction phase completed")

        return weather_data

    def transform_data(self, weather_data: dict[str, Any]) -> dict[str, pd.DataFrame]:
        """
        Transform raw API data into
        star-schema tables.
        """

        logger.info("Starting transformation phase")

        star_schema_data = self.transformer.transform(weather_data)

        logger.info("Transformation phase completed")

        return star_schema_data

    def load_warehouse(self, star_schema_data: dict[str, pd.DataFrame]) -> None:
        """
        Load transformed data into
        dimension and fact tables.
        """

        logger.info("Starting load phase")

        self.loader.load(star_schema_data)

        logger.info("Load phase completed")

    def run(self) -> None:
        """
        Execute complete ETL workflow.
        """

        try:
            logger.info("Starting ETL pipeline")

            weather_data = self.extract_data()

            star_schema_data = self.transform_data(weather_data)

            self.load_warehouse(star_schema_data)

            logger.info("ETL pipeline completed successfully")

        except Exception as e:
            logger.critical(f"ETL pipeline failed: {e}")

            raise
