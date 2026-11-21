import sqlite3
import logging
import joblib
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score
from src.utils.config import DB_PATH, MODEL_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

FEATURE_COLS = [
    "comuna",
    "zona",
    "via_tipo",
    "clase_vehiculo",
    "servicio",
    "hour_of_day",
    "is_weekend",
    "temperature_c",
    "precipitation_mm",
    "wind_speed_kmh",
    "traffic_density_index"
]
TARGET_COL = "is_severe_injury"


def load_training_data() -> pd.DataFrame:
    query = """
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
        f.traffic_density_index,
        f.is_severe_injury
    FROM fact_traffic_incident f
    JOIN dim_location l ON f.location_key = l.location_key
    JOIN dim_vehicle v ON f.vehicle_key = v.vehicle_key
    JOIN dim_date d ON f.date_key = d.date_key
    """
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query(query, conn)
    return df


def train_and_serialize():
    df = load_training_data()
    if len(df) < 50:
        raise ValueError("Insufficient training rows in fact_traffic_incident. Run the ETL pipeline first.")

    X = df[FEATURE_COLS]
    y = df[TARGET_COL]

    # Stratified train/test split to prevent leakage and maintain class balance
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.20, random_state=42, stratify=y
    )

    categorical_features = ["comuna", "zona", "via_tipo", "clase_vehiculo", "servicio"]
    numeric_features = [
        "hour_of_day",
        "is_weekend",
        "temperature_c",
        "precipitation_mm",
        "wind_speed_kmh",
        "traffic_density_index"
    ]

    categorical_transformer = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("onehot", OneHotEncoder(handle_unknown="ignore"))
    ])

    numeric_transformer = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler())
    ])

    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", categorical_transformer, categorical_features),
            ("num", numeric_transformer, numeric_features)
        ]
    )

    clf = Pipeline(steps=[
        ("preprocessor", preprocessor),
        ("classifier", RandomForestClassifier(
            n_estimators=150,
            max_depth=10,
            random_state=42,
            class_weight="balanced"
        ))
    ])

    clf.fit(X_train, y_train)

    y_pred = clf.predict(X_test)
    y_proba = clf.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, y_proba)

    logger.info(f"Model Evaluation ROC-AUC: {auc:.4f}")
    logger.info("\n" + classification_report(y_test, y_pred))

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    artifact = {
        "pipeline": clf,
        "feature_cols": FEATURE_COLS,
        "target_col": TARGET_COL,
        "metrics": {"roc_auc": round(float(auc), 4)}
    }
    joblib.dump(artifact, MODEL_PATH)
    logger.info(f"Serialized trained ML pipeline artifact to {MODEL_PATH}")


if __name__ == "__main__":
    train_and_serialize()
