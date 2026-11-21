import logging

from ml.train_model import train_and_save_model
from src.etl.extract import (
    extract_source_1_accidents,
    extract_source_2_homicides_history,
    extract_source_2_infrastructure,
    extract_source_3_weather_api,
)
from src.etl.load import load_to_warehouse
from src.etl.transform import transform_and_integrate
from src.quality.validator import validate_transformed_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def run_end_to_end_pipeline(train_ml: bool = True) -> dict:
    """
    Executes the batch ETL pipeline, data quality gate, Star Schema load,
    and optional ML model training.
    """
    logger.info("Starting Cali mobility ETL & ML pipeline...")

    df_accidents = extract_source_1_accidents()
    df_infra = extract_source_2_infrastructure()
    df_weather = extract_source_3_weather_api()
    df_homicides_hist = extract_source_2_homicides_history()

    df_enriched = transform_and_integrate(df_accidents, df_infra, df_weather)
    dq_report = validate_transformed_data(df_enriched)

    load_to_warehouse(df_enriched, df_homicides_hist=df_homicides_hist)

    ml_metrics = {}
    if train_ml:
        ml_metrics = train_and_save_model()

    summary = {
        "dq_report": dq_report,
        "ml_metrics": ml_metrics,
    }
    logger.info("Pipeline completed successfully.")
    return summary


if __name__ == "__main__":
    run_end_to_end_pipeline(train_ml=True)