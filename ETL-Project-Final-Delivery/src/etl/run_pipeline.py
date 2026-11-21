import logging
from src.etl.extract import (
    extract_source_1_accidents,
    extract_source_2_infrastructure,
    extract_source_3_weather_api,
)
from src.etl.transform import transform_and_integrate
from src.quality.validator import validate_enriched_dataset
from src.etl.load import load_dimensional_model

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def run_full_etl():
    logger.info("=== Step 1: Extracting 3 Data Sources ===")
    df1 = extract_source_1_accidents()
    df2 = extract_source_2_infrastructure()
    df3 = extract_source_3_weather_api()

    logger.info("=== Step 2: Transforming & Integrating Sources ===")
    df_enriched = transform_and_integrate(df1, df2, df3)

    logger.info("=== Step 3: Running Data Quality Validation Gate ===")
    validate_enriched_dataset(df_enriched)

    logger.info("=== Step 4: Loading Dimensional Model (Star Schema) ===")
    load_dimensional_model(df_enriched)
    logger.info("=== ETL Pipeline Completed Successfully ===")


if __name__ == "__main__":
    run_full_etl()
