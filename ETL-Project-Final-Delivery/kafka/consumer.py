import json
import logging
import sqlite3
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from src.utils.config import DB_PATH, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_ACCIDENTS

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def fetch_stream_events(limit: int = 100) -> list[dict]:
    """
    Reads input feature records from the SQLite star schema without leaking
    target or outcome columns (es_grave, tipo_confirmado_limpio).
    """
    query = """
    SELECT
        f.incident_id,
        t.anio,
        t.mes,
        t.dia_semana,
        t.es_fin_de_semana,
        f.hora_hecho,
        f.numero_vehiculos,
        u.comuna,
        u.zona,
        u.via_tipo,
        u.traffic_density_index,
        v.clase_accidente,
        v.vehiculos_implicados,
        v.medio_reporte,
        c.temperature_c,
        c.precipitation_mm,
        c.wind_speed_kmh
    FROM fact_siniestros f
    INNER JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
    INNER JOIN dim_ubicacion u ON f.ubicacion_key = u.ubicacion_key
    INNER JOIN dim_vehiculo_reporte v ON f.vehiculo_reporte_key = v.vehiculo_reporte_key
    INNER JOIN dim_clima c ON f.clima_key = c.clima_key
    ORDER BY f.siniestro_sk DESC
    LIMIT ?;
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query, (limit,)).fetchall()
    finally:
        conn.close()

    events = []
    for r in rows:
        payload = dict(r)
        payload["event_id"] = str(uuid.uuid4())
        payload["event_timestamp"] = datetime.now(timezone.utc).isoformat()
        events.append(payload)
    return events


def run_producer(delay_seconds: float = 0.5, limit: int = 100) -> None:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )
    events = fetch_stream_events(limit=limit)
    logger.info(f"Publishing {len(events)} events to topic '{KAFKA_TOPIC_ACCIDENTS}'...")

    try:
        for event in events:
            producer.send(KAFKA_TOPIC_ACCIDENTS, value=event)
            logger.info(
                f"Sent event_id={event['event_id']} incident_id={event['incident_id']} "
                f"comuna={event['comuna']} hour={event['hora_hecho']}"
            )
            time.sleep(delay_seconds)
        producer.flush()
    finally:
        producer.close()


if __name__ == "__main__":
    run_producer()