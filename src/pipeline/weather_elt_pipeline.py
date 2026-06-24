import pandas as pd

from src.extract.api_extractor import WeatherExtractor
from src.elt.staging_loader import StagingLoader
from src.elt.elt_transformer import ELTTransformer
from src.load.db_loader import WeatherLoader
from src.utils.logger import logger


class ELTPipeline:
    """
    End-to-end ELT workflow.

    Flow:
    1. Extract data from API
    2. Load raw data into staging
    3. Transform staged data
    4. Load dimension and fact tables
    5. Mark staging records as processed
    """

    def __init__(self) -> None:

        self.extractor = WeatherExtractor()

        self.staging_loader = StagingLoader()

        self.transformer = ELTTransformer()

        self.loader = WeatherLoader()

    def extract_to_staging(self) -> None:
        """
        Extract weather data from API and
        load raw payload into staging.
        """

        logger.info("Starting extraction phase")

        weather_data = self.extractor.get_weather_data()

        self.staging_loader.load_raw_payload(weather_data)

        logger.info("Extraction phase completed")

    def transform_staging(self) -> tuple[dict[str, pd.DataFrame], list[int]]:
        """
        Transform unprocessed staging records
        into star-schema tables.
        """

        logger.info("Starting transformation phase")

        (star_schema_data, staging_ids) = (
            self.transformer.transform_unprocessed_records()
        )

        logger.info("Transformation phase completed")

        return (star_schema_data, staging_ids)

    def load_warehouse(self, star_schema_data: dict[str, pd.DataFrame]) -> None:
        """
        Load transformed data into
        dimension and fact tables.
        """

        logger.info("Starting load phase")

        self.loader.load(star_schema_data)

        logger.info("Load phase completed")

    def mark_processed(self, staging_ids: list[int]) -> None:
        """
        Mark staging records as processed.
        """

        logger.info("Starting staging update phase")

        self.staging_loader.mark_as_processed(staging_ids)

        logger.info("Staging update phase completed")

    def run(self) -> None:
        """
        Execute complete ELT workflow.
        """

        try:
            logger.info("Starting ELT pipeline")

            self.extract_to_staging()

            (star_schema_data, staging_ids) = self.transform_staging()

            if not star_schema_data:
                logger.warning("No transformed data generated")

                return

            self.load_warehouse(star_schema_data)

            self.mark_processed(staging_ids)

            logger.info("ELT pipeline completed successfully")

        except Exception as e:
            logger.critical(f"ELT pipeline failed: {e}")

            raise
