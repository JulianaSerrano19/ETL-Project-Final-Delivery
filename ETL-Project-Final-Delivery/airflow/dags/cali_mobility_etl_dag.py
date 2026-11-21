from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.etl.extract import (
    extract_source_1_accidents,
    extract_source_2_homicides_history,
    extract_source_2_infrastructure,
    extract_source_3_weather_api,
)
from src.etl.load import load_to_warehouse
from src.etl.transform import transform_and_integrate
from src.quality.validator import validate_transformed_data
from src.utils.config import DATA_PROCESSED_DIR

DEFAULT_ARGS = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


def task_extract_and_transform(**context):
    df_acc = extract_source_1_accidents()
    df_infra = extract_source_2_infrastructure()
    df_weather = extract_source_3_weather_api()
    df_hom = extract_source_2_homicides_history()

    df_enriched = transform_and_integrate(df_acc, df_infra, df_weather)
    validate_transformed_data(df_enriched)

    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    enriched_path = DATA_PROCESSED_DIR / "dag_enriched_cache.pkl"
    homicides_path = DATA_PROCESSED_DIR / "dag_homicides_cache.pkl"

    df_enriched.to_pickle(enriched_path)
    if df_hom is not None:
        df_hom.to_pickle(homicides_path)

    context["ti"].xcom_push(key="enriched_path", value=str(enriched_path))
    context["ti"].xcom_push(key="homicides_path", value=str(homicides_path if df_hom is not None else ""))


def task_load_warehouse(**context):
    import pandas as pd

    ti = context["ti"]
    enriched_path = ti.xcom_pull(task_ids="extract_transform_validate", key="enriched_path")
    homicides_path = ti.xcom_pull(task_ids="extract_transform_validate", key="homicides_path")

    df_enriched = pd.read_pickle(enriched_path)
    df_hom = pd.read_pickle(homicides_path) if homicides_path else None
    load_to_warehouse(df_enriched, df_homicides_hist=df_hom)


def task_train_ml_model(**context):
    from ml.train_model import train_and_save_model

    metrics = train_and_save_model()
    context["ti"].xcom_push(key="ml_metrics", value=metrics)


with DAG(
    dag_id="cali_mobility_etl_ml_pipeline",
    default_args=DEFAULT_ARGS,
    description="End-to-end Cali mobility batch ETL, DQ validation, Star Schema load, and ML training",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["cali", "mobility", "etl", "ml", "kafka"],
) as dag:
    extract_transform_validate = PythonOperator(
        task_id="extract_transform_validate",
        python_callable=task_extract_and_transform,
    )

    load_star_schema = PythonOperator(
        task_id="load_star_schema",
        python_callable=task_load_warehouse,
    )

    train_severity_model = PythonOperator(
        task_id="train_severity_model",
        python_callable=task_train_ml_model,
    )

    extract_transform_validate >> load_star_schema >> train_severity_model