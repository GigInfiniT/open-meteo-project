from src.elt.staging_loader import StagingLoader

loader = StagingLoader()

sample_data = {
    "daily": {
        "time": ["2026-06-17"],
        "temperature_2m_max": [30],
        "temperature_2m_min": [20],
        "precipitation_sum": [1],
        "wind_speed_10m_max": [5],
    }
}

loader.load_raw_payload(sample_data)

print("Done")
