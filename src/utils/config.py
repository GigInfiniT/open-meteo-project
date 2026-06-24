from dataclasses import dataclass
from dotenv import load_dotenv
import os

# Load environment variables from .env into the process environment.
load_dotenv()


@dataclass
class Config:
    """Application configuration loaded from environment variables."""

    # API configuration
    API_BASE_URL: str = os.getenv(
        "API_BASE_URL", "https://api.open-meteo.com/v1/forecast"
    )

    LATITUDE: float = float(os.getenv("LATITUDE", 9.0765))

    LONGITUDE: float = float(os.getenv("LONGITUDE", 7.3986))

    LOCATION_NAME: str = os.getenv("LOCATION_NAME", "Abuja")

    # Database configuration
    DB_HOST: str = os.getenv("DB_HOST", "localhost")

    DB_PORT: int = int(os.getenv("DB_PORT", 5432))

    DB_NAME: str = os.getenv("DB_NAME", "weather_db")

    DB_USER: str = os.getenv("DB_USER", "postgres")

    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "")

    WEATHER_FIELDS: str = os.getenv(
        "WEATHER_FIELDS",
        "temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max",
    )


config = Config()
