import json
import sqlite3
import logging
from datetime import datetime, timezone
import joblib
import pandas as pd
from kafka import KafkaConsumer
from src.utils.config import DB_PATH, MODEL_PATH, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_ACCIDENTS

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

REQUIRED_FEATURES = [
    "comuna", "zona", "via_tipo", "clase_vehiculo", "servicio",
    "hour_of_day", "is_weekend", "temperature_c", "precipitation_mm",
    "wind_speed_kmh", "traffic_density_index"
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS streaming_incident_predictions (
    event_id TEXT PRIMARY KEY,
    incident_id TEXT,
    event_timestamp TEXT,
    comuna TEXT,
    clase_vehiculo TEXT,
    hour_of_day INTEGER,
    temperature_c REAL,
    precipitation_mm REAL,
    traffic_density_index REAL,
    predicted_severe_risk INTEGER,
    severe_probability REAL,
    risk_level TEXT,
    processed_at TEXT
)
"""


def classify_risk(prob: float) -> str:
    if prob >= 0.70:
        return "HIGH"
    elif prob >= 0.45:
        return "MEDIUM"
    return "LOW"


def validate_message(msg: dict) -> bool:
    if not isinstance(msg, dict):
        return False
    if "event_id" not in msg or "incident_id" not in msg or "features" not in msg:
        return False
    features = msg["features"]
    if not isinstance(features, dict):
        return False
    return all(col in features and features[col] is not None for col in REQUIRED_FEATURES)


def ensure_predictions_table():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(CREATE_TABLE_SQL)
        conn.commit()


def persist_prediction(record: dict):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """INSERT OR IGNORE INTO streaming_incident_predictions
               (event_id, incident_id, event_timestamp, comuna, clase_vehiculo,
                hour_of_day, temperature_c, precipitation_mm, traffic_density_index,
                predicted_severe_risk, severe_probability, risk_level, processed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                record["event_id"],
                record["incident_id"],
                record["event_timestamp"],
                record["comuna"],
                record["clase_vehiculo"],
                record["hour_of_day"],
                record["temperature_c"],
                record["precipitation_mm"],
                record["traffic_density_index"],
                record["predicted_severe_risk"],
                record["severe_probability"],
                record["risk_level"],
                record["processed_at"]
            )
        )
        conn.commit()


def run_consumer():
    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"Serialized model not found at {MODEL_PATH}. Run ml/train_model.py first.")

    ensure_predictions_table()

    artifact = joblib.load(MODEL_PATH)
    pipeline = artifact["pipeline"]
    feature_cols = artifact["feature_cols"]
    logger.info(f"Loaded serialized ML pipeline from {MODEL_PATH} (ROC-AUC={artifact.get('metrics', {}).get('roc_auc')})")

    consumer = KafkaConsumer(
        KAFKA_TOPIC_ACCIDENTS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="cali-incident-inference-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    logger.info(f"Listening for streaming events on topic '{KAFKA_TOPIC_ACCIDENTS}'...")
    for message in consumer:
        payload = message.value
        if not validate_message(payload):
            logger.warning(f"Skipping malformed Kafka message: {payload}")
            continue

        features = payload["features"]
        input_df = pd.DataFrame([{col: features[col] for col in feature_cols}])

        pred_class = int(pipeline.predict(input_df)[0])
        pred_proba = float(pipeline.predict_proba(input_df)[0][1])
        risk_label = classify_risk(pred_proba)

        result = {
            "event_id": payload["event_id"],
            "incident_id": payload["incident_id"],
            "event_timestamp": payload.get("event_timestamp", datetime.now(timezone.utc).isoformat()),
            "comuna": features["comuna"],
            "clase_vehiculo": features["clase_vehiculo"],
            "hour_of_day": int(features["hour_of_day"]),
            "temperature_c": float(features["temperature_c"]),
            "precipitation_mm": float(features["precipitation_mm"]),
            "traffic_density_index": float(features["traffic_density_index"]),
            "predicted_severe_risk": pred_class,
            "severe_probability": round(pred_proba, 4),
            "risk_level": risk_label,
            "processed_at": datetime.now(timezone.utc).isoformat()
        }

        persist_prediction(result)
        logger.info(
            f"Predicted incident={result['incident_id']} | risk={risk_label} "
            f"(prob={result['severe_probability']:.2%}) | comuna={result['comuna']}"
        )


if __name__ == "__main__":
    run_consumer()