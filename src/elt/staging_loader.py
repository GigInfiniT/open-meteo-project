import json
import pandas as pd

from sqlalchemy import create_engine, text, bindparam
from sqlalchemy.exc import SQLAlchemyError

from src.utils.config import config
from src.utils.logger import logger


class StagingLoader:
    """
    Handles ELT staging operations.

    Responsibilities:
        - Load raw API payloads into the staging table
        - Retrieve unprocessed staging records
        - Mark staging records as processed after a successful load
    """

    def __init__(self) -> None:
        # Create a reusable SQLAlchemy engine for PostgreSQL connections.
        # pool_pre_ping helps avoid stale connections, while pool_recycle
        # refreshes long-lived connections periodically.
        self.engine = create_engine(
            f"postgresql+psycopg2://"
            f"{config.DB_USER}:"
            f"{config.DB_PASSWORD}@"
            f"{config.DB_HOST}:"
            f"{config.DB_PORT}/"
            f"{config.DB_NAME}",
            pool_pre_ping=True,
            pool_recycle=3600,
        )

    def load_raw_payload(self, weather_data: dict) -> None:
        """
        Load a raw API response into the staging table.

        Args:
            weather_data: Raw weather payload returned by the API.
        """
        logger.info("Loading raw payload into staging table")

        # Store the raw API response as JSONB in PostgreSQL so that it can be
        # transformed later during the ELT process.
        query = text(
            """
            INSERT INTO stg_weather_raw (raw_payload)
            VALUES (CAST(:raw_payload AS JSONB))
            """
        )

        try:
            # Use a transaction block so the insert is committed automatically
            # if successful, or rolled back if an exception occurs.
            with self.engine.begin() as conn:
                conn.execute(query, {"raw_payload": json.dumps(weather_data)})

            logger.info("Raw payload loaded successfully")

        except SQLAlchemyError as e:
            logger.error(f"Failed loading raw payload: {e}")
            raise

    def get_unprocessed_records(self) -> pd.DataFrame:
        """
        Retrieve raw staging records that have not yet been processed.

        Returns:
            A DataFrame containing:
                - staging_id
                - raw_payload
                - extraction_timestamp
        """
        logger.info("Retrieving unprocessed staging records")

        query = """
            SELECT
                staging_id,
                raw_payload,
                extraction_timestamp
            FROM stg_weather_raw
            WHERE processed = FALSE
            ORDER BY staging_id
        """

        try:
            # Use a raw DBAPI connection because this works reliably with the
            # current Airflow/Pandas environment for pd.read_sql_query().
            raw_conn = self.engine.raw_connection()
            try:
                df = pd.read_sql_query(query, raw_conn)
            finally:
                # Ensure the connection is always returned/closed even if the
                # query fails.
                raw_conn.close()

            return df

        except Exception as e:
            logger.error(f"Failed retrieving staging records: {e}")
            raise

    def mark_as_processed(self, staging_ids: list[int]) -> None:
        """
        Mark staging records as processed after a successful transformation
        and warehouse load.

        Args:
            staging_ids: List of staging record IDs to mark as processed.
        """
        if not staging_ids:
            logger.info("No staging IDs provided")
            return

        logger.info(f"Marking {len(staging_ids)} staging records as processed")

        # Use SQLAlchemy's expanding bind parameter so the Python list is
        # safely expanded into the SQL IN clause at execution time.
        query = text(
            """
                UPDATE stg_weather_raw
                SET processed = TRUE
                WHERE staging_id IN :staging_ids
                """
        ).bindparams(bindparam("staging_ids", expanding=True))

        try:
            with self.engine.begin() as conn:
                conn.execute(query, {"staging_ids": staging_ids})

            logger.info("Staging records marked as processed successfully")

        except SQLAlchemyError as e:
            logger.error(f"Failed updating staging records: {e}")
            raise
