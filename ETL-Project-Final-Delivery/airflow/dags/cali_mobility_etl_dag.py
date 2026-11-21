from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd

from src.etl.extract import (
    extract_source_1_accidents,
    extract_source_2_infrastructure,
    extract_source_3_weather_api,
)
from src.etl.transform import transform_and_integrate
from src.quality.validator import validate_enriched_dataset
from src.etl.load import load_dimensional_model
from src.utils.config import DATA_RAW_DIR, DATA_PROCESSED_DIR

default_args = {
    "owner": "data_engineering_g51",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def task_extract_accidents():
    df = extract_source_1_accidents()
    df.to_pickle(DATA_RAW_DIR / "tmp_src1.pkl")


def task_extract_infrastructure():
    df = extract_source_2_infrastructure()
    df.to_pickle(DATA_RAW_DIR / "tmp_src2.pkl")


def task_extract_weather():
    df = extract_source_3_weather_api()
    df.to_pickle(DATA_RAW_DIR / "tmp_src3.pkl")


def task_transform_merge():
    df1 = pd.read_pickle(DATA_RAW_DIR / "tmp_src1.pkl")
    df2 = pd.read_pickle(DATA_RAW_DIR / "tmp_src2.pkl")
    df3 = pd.read_pickle(DATA_RAW_DIR / "tmp_src3.pkl")
    transform_and_integrate(df1, df2, df3)


def task_data_quality_gate():
    df_staged = pd.read_csv(DATA_PROCESSED_DIR / "enriched_incidents_staged.csv")
    validate_enriched_dataset(df_staged)


def task_load_warehouse():
    df_staged = pd.read_csv(DATA_PROCESSED_DIR / "enriched_incidents_staged.csv")
    load_dimensional_model(df_staged)


def task_publish_kafka_batch():
    from kafka.producer import run_producer
    run_producer(delay_seconds=0.5, max_messages=30)


with DAG(
    dag_id="cali_traffic_etl_and_streaming_pipeline",
    default_args=default_args,
    description="End-to-end ETL (3 sources), Great Expectations validation gate, Star Schema load, and Kafka stream trigger",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "cali", "dimensional-model", "kafka"],
) as dag:

    extract_1 = PythonOperator(task_id="extract_source1_accidents", python_callable=task_extract_accidents)
    extract_2 = PythonOperator(task_id="extract_source2_infrastructure", python_callable=task_extract_infrastructure)
    extract_3 = PythonOperator(task_id="extract_source3_weather_api", python_callable=task_extract_weather)

    transform_merge = PythonOperator(task_id="transform_and_integrate_sources", python_callable=task_transform_merge)
    dq_gate = PythonOperator(task_id="validate_data_quality_gate", python_callable=task_data_quality_gate)
    load_dw = PythonOperator(task_id="load_dimensional_model", python_callable=task_load_warehouse)
    kafka_stream = PythonOperator(task_id="publish_fact_events_to_kafka", python_callable=task_publish_kafka_batch)

    [extract_1, extract_2, extract_3] >> transform_merge >> dq_gate >> load_dw >> kafka_stream
