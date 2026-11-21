import json
import time
import uuid
import sqlite3
import logging
from datetime import datetime, timezone
import pandas as pd
from kafka import KafkaProducer
from src.utils.config import DB_PATH, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_ACCIDENTS

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def fetch_stream_candidates(limit: int = 100) -> pd.DataFrame:
    """
    Queries the dimensional model to build inference feature payloads.
    Explicitly excludes target variables (is_severe_injury, victims_count) to avoid target leakage.
    """
    query = f"""
    SELECT
        f.incident_id,
        l.comuna,
        l.zona,
        l.via_tipo,
        v.clase_vehiculo,
        v.servicio,
        f.hour_of_day,
        d.is_weekend,
        f.temperature_c,
        f.precipitation_mm,
        f.wind_speed_kmh,
        f.traffic_density_index
    FROM fact_traffic_incident f
    JOIN dim_location l ON f.location_key = l.location_key
    JOIN dim_vehicle v ON f.vehicle_key = v.vehicle_key
    JOIN dim_date d ON f.date_key = d.date_key
    ORDER BY RANDOM()
    LIMIT {limit}
    """
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(query, conn)


def run_producer(delay_seconds: float = 1.5, max_messages: int = 100):
    df = fetch_stream_candidates(limit=max_messages)
    if df.empty:
        logger.error("No records found in fact_traffic_incident. Run ETL pipeline first.")
        return

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None
    )

    logger.info(f"Publishing {len(df)} traffic events to Kafka topic '{KAFKA_TOPIC_ACCIDENTS}'...")
    for idx, row in df.iterrows():
        event_payload = {
            "event_id": str(uuid.uuid4()),
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "incident_id": row["incident_id"],
            "features": {
                "comuna": row["comuna"],
                "zona": row["zona"],
                "via_tipo": row["via_tipo"],
                "clase_vehiculo": row["clase_vehiculo"],
                "servicio": row["servicio"],
                "hour_of_day": int(row["hour_of_day"]),
                "is_weekend": int(row["is_weekend"]),
                "temperature_c": float(row["temperature_c"]),
                "precipitation_mm": float(row["precipitation_mm"]),
                "wind_speed_kmh": float(row["wind_speed_kmh"]),
                "traffic_density_index": float(row["traffic_density_index"])
            }
        }
        producer.send(KAFKA_TOPIC_ACCIDENTS, key=row["incident_id"], value=event_payload)
        producer.flush()
        logger.info(
            f"[{idx + 1}/{len(df)}] Sent event_id={event_payload['event_id']} | "
            f"incident={row['incident_id']} | vehicle={row['clase_vehiculo']} | comuna={row['comuna']}"
        )
        time.sleep(delay_seconds)

    producer.close()
    logger.info("Kafka producer finished streaming batch.")


if __name__ == "__main__":
    run_producer()
