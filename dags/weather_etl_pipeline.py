from datetime import datetime, timedelta
from pathlib import Path
import json
import sys
import pandas as pd

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context

# Make project importable inside Airflow
PROJECT_ROOT = "/home/ugo/repos/open-meteo-project"
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from src.extract.api_extractor import WeatherExtractor
from src.transform.transformer import WeatherTransformer
from src.load.db_loader import WeatherLoader


default_args = {
    "owner": "ugo",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_etl_pipeline",
    description="Weather ETL pipeline for Open-Meteo data",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["weather", "etl"],
    max_active_runs=1,
    render_template_as_native_obj=True,
) as dag:

    @task
    def extract_task() -> str:
        """
        Extract raw weather data from API and save it as JSON
        in a run-specific temporary folder.
        Returns the path to the raw JSON file.
        """
        context = get_current_context()
        run_id = context["run_id"]
        safe_run_id = (
            run_id.replace(":", "_")
            .replace("+", "_")
            .replace("/", "_")
        )

        run_dir = Path("/tmp/weather_etl") / safe_run_id
        run_dir.mkdir(parents=True, exist_ok=True)

        extractor = WeatherExtractor()
        weather_data = extractor.get_weather_data()

        raw_data_path = run_dir / "raw_weather_data.json"

        with open(raw_data_path, "w") as f:
            json.dump(weather_data, f)

        return str(raw_data_path)

    @task
    def transform_task(raw_data_path: str) -> dict[str, str]:
        """
        Read raw JSON from disk, transform it into star-schema tables,
        save each table to CSV in the same run-specific folder,
        and return a mapping of table name -> file path.
        """
        raw_path = Path(raw_data_path)
        run_dir = raw_path.parent

        with open(raw_path, "r") as f:
            weather_data = json.load(f)

        transformer = WeatherTransformer()
        star_schema_data = transformer.transform(weather_data)

        saved_paths: dict[str, str] = {}

        for table_name, df in star_schema_data.items():
            path = run_dir / f"{table_name}.csv"
            df.to_csv(path, index=False)
            saved_paths[table_name] = str(path)

        return saved_paths

    @task
    def load_task(saved_paths: dict[str, str], raw_data_path: str) -> None:
        """
        Read transformed CSV files from disk, load them into the warehouse,
        then clean up temporary files and the run directory after a successful load.
        """
        star_schema_data = {
            table_name: pd.read_csv(path)
            for table_name, path in saved_paths.items()
        }

        loader = WeatherLoader()
        loader.load(star_schema_data)

        # Cleanup only after successful load
        files_to_delete = list(saved_paths.values()) + [raw_data_path]

        for file_path in files_to_delete:
            path_obj = Path(file_path)
            if path_obj.exists():
                path_obj.unlink()

        # Remove the run directory if empty
        run_dir = Path(raw_data_path).parent
        if run_dir.exists():
            try:
                run_dir.rmdir()
            except OSError:
                pass

    raw_file = extract_task()
    transformed_files = transform_task(raw_file)
    load_task(transformed_files, raw_file)
