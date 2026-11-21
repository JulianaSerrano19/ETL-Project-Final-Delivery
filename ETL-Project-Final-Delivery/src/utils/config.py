import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_RAW_DIR = BASE_DIR / "data" / "raw"
DATA_PROCESSED_DIR = BASE_DIR / "data" / "processed"
DB_PATH = BASE_DIR / "database" / "cali_mobility_dw.db"
DB_URI = f"sqlite:///{DB_PATH}"

MODEL_PATH = BASE_DIR / "ml" / "models" / "accident_severity_pipeline.pkl"
REPORT_OUTPUT_DIR = BASE_DIR / "reports" / "static_output"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_ACCIDENTS = os.getenv("KAFKA_TOPIC_ACCIDENTS", "cali_traffic_incidents_stream")

# Public API (Open-Meteo Historical / Forecast Weather API for Cali, Colombia)
CALI_LAT = 3.4516
CALI_LON = -76.5320
OPEN_METEO_API_URL = "https://api.open-meteo.com/v1/forecast"
