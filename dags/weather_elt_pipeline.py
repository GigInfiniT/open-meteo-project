from datetime import datetime, timedelta
from pathlib import Path
import sys
import pandas as pd

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context

# Make project importable inside Airflow
PROJECT_ROOT = "/home/ugo/repos/open-meteo-project"
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from src.pipeline.weather_elt_pipeline import ELTPipeline


default_args = {
    "owner": "ugo",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_elt_pipeline",
    description="Weather ELT pipeline for Open-Meteo data",
    default_args=default_args,
    start_date=datetime(2026, 6, 1),
    schedule="@daily",
    catchup=False,
    tags=["weather", "elt"],
    max_active_runs=1,
    render_template_as_native_obj=True,
) as dag:

    @task
    def extract_to_staging_task() -> str:
        """
        Extract raw weather data from API and load it into the staging table.
        Returns a confirmation token so downstream tasks depend on this task.
        """
        pipeline = ELTPipeline()
        pipeline.extract_to_staging()
        return "extract_complete"

    @task
    def transform_staging_task(_extract_status: str) -> dict:
        """
        Read unprocessed staging records, transform them into star-schema tables,
        save them as CSVs in a run-specific temp folder, and return:
            - saved_paths
            - staging_ids
            - run_dir
        """
        context = get_current_context()
        run_id = context["run_id"]
        safe_run_id = (
            run_id.replace(":", "_")
            .replace("+", "_")
            .replace("/", "_")
        )

        run_dir = Path("/tmp/weather_elt") / safe_run_id
        run_dir.mkdir(parents=True, exist_ok=True)

        pipeline = ELTPipeline()
        star_schema_data, staging_ids = pipeline.transform_staging()

        # Nothing to transform
        if not star_schema_data:
            return {
                "saved_paths": {},
                "staging_ids": staging_ids,
                "run_dir": str(run_dir),
            }

        saved_paths: dict[str, str] = {}

        for table_name, df in star_schema_data.items():
            path = run_dir / f"{table_name}.csv"
            df.to_csv(path, index=False)
            saved_paths[table_name] = str(path)

        return {
            "saved_paths": saved_paths,
            "staging_ids": staging_ids,
            "run_dir": str(run_dir),
        }

    @task
    def load_warehouse_task(transform_output: dict) -> str:
        """
        Load transformed CSVs into the warehouse.
        Returns a confirmation token only if load succeeds.
        """
        saved_paths = transform_output["saved_paths"]

        # No transformed data -> nothing to load
        if not saved_paths:
            return "no_data_to_load"

        star_schema_data = {
            table_name: pd.read_csv(path)
            for table_name, path in saved_paths.items()
        }

        pipeline = ELTPipeline()
        pipeline.load_warehouse(star_schema_data)

        return "load_complete"

    @task
    def mark_processed_task(transform_output: dict, _load_status: str) -> None:
        """
        Mark staging rows as processed only after warehouse load succeeds,
        then clean up temporary CSV files and run directory.
        """
        staging_ids = transform_output["staging_ids"]
        saved_paths = transform_output["saved_paths"]
        run_dir = Path(transform_output["run_dir"])

        # Only mark processed if there was actually transformed data saved
        if not saved_paths:
            return

        pipeline = ELTPipeline()
        pipeline.mark_processed(staging_ids)

        # Cleanup local CSV files
        for file_path in saved_paths.values():
            path_obj = Path(file_path)
            if path_obj.exists():
                path_obj.unlink()

        # Remove run directory if empty
        if run_dir.exists():
            try:
                run_dir.rmdir()
            except OSError:
                pass

    # TaskFlow dependency chain
    extract_status = extract_to_staging_task()
    transform_output = transform_staging_task(extract_status)
    load_status = load_warehouse_task(transform_output)
    mark_processed_task(transform_output, load_status)
