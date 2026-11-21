import json
import logging
import sqlite3
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from src.utils.config import BASE_DIR, DB_PATH, MODEL_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

NUMERIC_FEATURES = [
    "anio",
    "mes",
    "dia_semana",
    "es_fin_de_semana",
    "hora_hecho",
    "numero_vehiculos",
    "traffic_density_index",
    "temperature_c",
    "precipitation_mm",
    "wind_speed_kmh",
]

CATEGORICAL_FEATURES = [
    "clase_accidente",
    "vehiculos_implicados",
    "medio_reporte",
    "comuna",
    "zona",
    "via_tipo",
]

TARGET_COLUMN = "es_grave"


def load_training_dataframe(db_path: Path = DB_PATH) -> pd.DataFrame:
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
        c.wind_speed_kmh,
        f.es_grave
    FROM fact_siniestros f
    INNER JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
    INNER JOIN dim_ubicacion u ON f.ubicacion_key = u.ubicacion_key
    INNER JOIN dim_vehiculo_reporte v ON f.vehiculo_reporte_key = v.vehiculo_reporte_key
    INNER JOIN dim_clima c ON f.clima_key = c.clima_key
    WHERE f.es_grave IN (0, 1);
    """
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()
    return df


def build_pipeline() -> Pipeline:
    numeric_transformer = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
    ])

    categorical_transformer = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("onehot", OneHotEncoder(handle_unknown="ignore")),
    ])

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, NUMERIC_FEATURES),
            ("cat", categorical_transformer, CATEGORICAL_FEATURES),
        ],
        remainder="drop",
    )

    classifier = RandomForestClassifier(
        n_estimators=120,
        max_depth=14,
        min_samples_leaf=10,
        class_weight="balanced",
        random_state=42,
        n_jobs=-1,
    )

    return Pipeline(steps=[
        ("preprocessor", preprocessor),
        ("classifier", classifier),
    ])


def train_and_save_model(
    db_path: Path = DB_PATH,
    model_path: Path = MODEL_PATH,
    sample_limit: int | None = 120000,
) -> dict:
    df = load_training_dataframe(db_path)
    if df.empty:
        raise ValueError("No training records found in fact_siniestros.")

    if sample_limit and len(df) > sample_limit:
        df = df.sample(n=sample_limit, random_state=42)

    X = df[NUMERIC_FEATURES + CATEGORICAL_FEATURES].copy()
    y = df[TARGET_COLUMN].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    pipeline = build_pipeline()
    logger.info("Training RandomForestClassifier severity pipeline...")
    pipeline.fit(X_train, y_train)

    y_pred = pipeline.predict(X_test)
    y_prob = pipeline.predict_proba(X_test)[:, 1]

    metrics = {
        "rows_used": int(len(df)),
        "accuracy": round(float(accuracy_score(y_test, y_pred)), 4),
        "precision": round(float(precision_score(y_test, y_pred, zero_division=0)), 4),
        "recall": round(float(recall_score(y_test, y_pred, zero_division=0)), 4),
        "f1_score": round(float(f1_score(y_test, y_pred, zero_division=0)), 4),
        "roc_auc": round(float(roc_auc_score(y_test, y_prob)), 4),
    }

    model_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipeline, model_path)

    metrics_path = model_path.parent / "metrics.json"
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    logger.info(f"Model saved to {model_path}. Metrics: {metrics}")
    return metrics


def predict_records(records: list[dict], model_path: Path = MODEL_PATH) -> list[dict]:
    model = joblib.load(model_path)
    df_in = pd.DataFrame(records)
    features = df_in[NUMERIC_FEATURES + CATEGORICAL_FEATURES]
    preds = model.predict(features)
    probs = model.predict_proba(features)[:, 1]

    results = []
    for idx, row in df_in.iterrows():
        prob = float(probs[idx])
        pred = int(preds[idx])
        alert_level = "HIGH" if prob >= 0.65 else ("MEDIUM" if prob >= 0.40 else "LOW")
        results.append({
            "incident_id": row.get("incident_id", f"INC-{idx}"),
            "predicted_is_severe": pred,
            "severe_probability": round(prob, 4),
            "alert_level": alert_level,
        })
    return results


if __name__ == "__main__":
    train_and_save_model()