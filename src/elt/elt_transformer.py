import pandas as pd

from src.elt.staging_loader import StagingLoader
from src.transform.transformer import WeatherTransformer
from src.utils.logger import logger


class ELTTransformer:
    """
    Reads raw weather payloads from the staging
    layer and transforms them into analytical
    star-schema tables.
    """

    def __init__(self) -> None:

        self.staging_loader = StagingLoader()

        self.transformer = WeatherTransformer()

    def transform_unprocessed_records(
        self,
    ) -> tuple[dict[str, pd.DataFrame], list[int]]:
        """
        Retrieve unprocessed staging records,
        transform them using the existing ETL
        transformation logic, and return:

        - star schema tables
        - processed staging IDs
        """

        records = self.staging_loader.get_unprocessed_records()

        if records.empty:
            logger.info("No unprocessed staging records found")

            return {}, []

        staging_ids = records["staging_id"].tolist()

        logger.info(f"Transforming {len(staging_ids)} staging record(s)")

        dim_date_list = []

        dim_location_list = []

        fact_weather_list = []

        for _, row in records.iterrows():
            payload = row["raw_payload"]

            transformed_batch = self.transformer.transform(payload)

            dim_date_list.append(transformed_batch["dim_date"])

            dim_location_list.append(transformed_batch["dim_location"])

            fact_weather_list.append(transformed_batch["fact_weather"])

        if not fact_weather_list:
            logger.warning("No transformed records generated")

            return {}, staging_ids

        star_schema_data = {
            "dim_date": (pd.concat(dim_date_list, ignore_index=True).drop_duplicates()),
            "dim_location": (
                pd.concat(dim_location_list, ignore_index=True).drop_duplicates()
            ),
            "fact_weather": (pd.concat(fact_weather_list, ignore_index=True)),
        }

        logger.info(f"Generated {len(star_schema_data['dim_date'])} dim_date rows")

        logger.info(
            f"Generated {len(star_schema_data['dim_location'])} dim_location rows"
        )

        logger.info(
            f"Generated {len(star_schema_data['fact_weather'])} fact_weather rows"
        )

        return (star_schema_data, staging_ids)
