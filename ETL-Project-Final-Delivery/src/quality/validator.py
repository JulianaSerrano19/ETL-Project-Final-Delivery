import json
import sqlite3
import logging
from datetime import datetime
import pandas as pd
from src.utils.config import DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class DataQualityException(Exception):
    """Raised when critical data quality expectations fail, blocking warehouse load."""
    pass


def validate_enriched_dataset(df: pd.DataFrame) -> dict:
    """
    Executes critical Great Expectations-aligned validation checks on the integrated dataset.
    Records audit results in dq_validation_logs and raises DataQualityException on critical failures.
    """
    expectations = [
        {
            "name": "expect_table_row_count_to_be_between",
            "critical": True,
            "passed": bool(100 <= len(df) <= 1_000_000),
            "details": f"Row count = {len(df)}"
        },
        {
            "name": "expect_column_values_to_be_unique_incident_id",
            "critical": True,
            "passed": bool(df["incident_id"].is_unique and df["incident_id"].notna().all()),
            "details": f"Unique incident_ids = {df['incident_id'].nunique()}/{len(df)}"
        },
        {
            "name": "expect_column_values_to_not_be_null_core_keys",
            "critical": True,
            "passed": bool(df[["date_key", "comuna", "barrio", "clase_vehiculo", "hour_of_day"]].notna().all().all()),
            "details": "Null check across core dimensional foreign/business keys"
        },
        {
            "name": "expect_column_values_to_be_between_hour_of_day",
            "critical": True,
            "passed": bool(df["hour_of_day"].between(0, 23).all()),
            "details": f"Hour range: [{df['hour_of_day'].min()}, {df['hour_of_day'].max()}]"
        },
        {
            "name": "expect_column_values_to_be_between_temperature_c",
            "critical": True,
            "passed": bool(df["temperature_c"].between(5.0, 45.0).all()),
            "details": f"Temp range: [{df['temperature_c'].min()}, {df['temperature_c'].max()}]"
        },
        {
            "name": "expect_column_values_to_be_between_traffic_density",
            "critical": True,
            "passed": bool(df["traffic_density_index"].between(0.0, 1.0).all()),
            "details": f"Traffic density range: [{df['traffic_density_index'].min()}, {df['traffic_density_index'].max()}]"
        },
        {
            "name": "expect_column_values_to_be_in_set_target_is_severe_injury",
            "critical": True,
            "passed": bool(set(df["is_severe_injury"].unique()).issubset({0, 1})),
            "details": f"Target classes present: {sorted(df['is_severe_injury'].unique().tolist())}"
        }
    ]

    total = len(expectations)
    passed_count = sum(1 for e in expectations if e["passed"])
    failed_count = total - passed_count
    overall_success = failed_count == 0

    report = {
        "run_timestamp": datetime.utcnow().isoformat(),
        "suite_name": "cali_traffic_enriched_dq_suite",
        "success": overall_success,
        "evaluated_expectations": total,
        "successful_expectations": passed_count,
        "unsuccessful_expectations": failed_count,
        "results": expectations
    }

    # Persist validation audit log to database
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dq_validation_logs (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_timestamp TEXT NOT NULL,
                suite_name TEXT NOT NULL,
                success INTEGER NOT NULL,
                evaluated_expectations INTEGER NOT NULL,
                successful_expectations INTEGER NOT NULL,
                unsuccessful_expectations INTEGER NOT NULL,
                details TEXT
            )
        """)
        conn.execute(
            """INSERT INTO dq_validation_logs
               (run_timestamp, suite_name, success, evaluated_expectations, successful_expectations, unsuccessful_expectations, details)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                report["run_timestamp"],
                report["suite_name"],
                int(overall_success),
                total,
                passed_count,
                failed_count,
                json.dumps(expectations)
            )
        )
        conn.commit()

    if not overall_success:
        failed_names = [e["name"] for e in expectations if not e["passed"]]
        logger.error(f"DATA QUALITY GATE FAILED: {failed_names}")
        raise DataQualityException(f"Critical DQ checks failed: {failed_names}. Aborting Data Warehouse load.")

    logger.info(f"DATA QUALITY GATE PASSED: {passed_count}/{total} expectations succeeded.")
    return report
