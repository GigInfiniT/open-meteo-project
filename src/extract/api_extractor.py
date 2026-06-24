import requests
from typing import Any

from src.utils.config import config
from src.utils.logger import logger


class WeatherExtractor:
    """
    Handles extraction of weather forecast data from the Open-Meteo API.

    Responsibilities:
        - Build API request parameters
        - Send API requests
        - Validate API responses
        - Log extraction events
        - Handle extraction-related exceptions
    """

    def __init__(self) -> None:
        self.base_url = config.API_BASE_URL

    def build_params(self) -> dict[str, Any]:
        """
        Build API request parameters.
        """

        return {
            "latitude": config.LATITUDE,
            "longitude": config.LONGITUDE,
            "daily": config.WEATHER_FIELDS,
            "timezone": "auto",
        }

    def validate_response(self, response: requests.Response) -> dict[str, Any]:
        """
        Validate API response and return JSON data.
        """

        if response.status_code != 200:
            raise ValueError(f"API returned status code {response.status_code}")

        try:
            data = response.json()

        except ValueError as exc:
            raise ValueError("API response is not valid JSON") from exc

        if not data:
            raise ValueError("API returned an empty response")

        if "daily" not in data:
            raise ValueError("'daily' field missing from API response")

        return data

    def get_weather_data(self) -> dict[str, Any]:
        """
        Extract weather data from Open-Meteo API.
        """

        params = self.build_params()

        logger.info("Starting weather data extraction")

        try:
            response = requests.get(self.base_url, params=params, timeout=30)

            response.raise_for_status()

            data = self.validate_response(response)

            logger.info("Weather data extracted successfully")

            return data

        except requests.exceptions.Timeout as exc:
            logger.error(f"API request timed out: {exc}")

            raise

        except requests.exceptions.ConnectionError as exc:
            logger.error(f"API connection failed: {exc}")

            raise

        except requests.exceptions.RequestException as exc:
            logger.error(f"API request failed: {exc}")

            raise

        except Exception as exc:
            logger.error(f"Unexpected extraction error: {exc}")

            raise
