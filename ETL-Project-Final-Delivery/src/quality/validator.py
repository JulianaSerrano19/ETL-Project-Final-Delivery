import json
import sqlite3
import logging
from datetime import datetime, timezone
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
    required_cols = [
        "incident_id", "date_key", "comuna", "barrio", "clase_vehiculo",
        "hour_of_day", "temperature_c", "traffic_density_index", "is_severe_injury"
    ]
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise DataQualityException(f"Missing required columns for DQ validation: {missing_cols}")

    row_count = len(df)
    hour_min = df["hour_of_day"].min() if row_count > 0 else None
    hour_max = df["hour_of_day"].max() if row_count > 0 else None
    temp_min = df["temperature_c"].min() if row_count > 0 else None
    temp_max = df["temperature_c"].max() if row_count > 0 else None
    dens_min = df["traffic_density_index"].min() if row_count > 0 else None
    dens_max = df["traffic_density_index"].max() if row_count > 0 else None

    expectations = [
        {
            "name": "expect_table_row_count_to_be_between",
            "critical": True,
            "passed": bool(100 <= row_count <= 1_000_000),
            "details": f"Row count = {row_count}"
        },
        {
            "name": "expect_column_values_to_be_unique_incident_id",
            "critical": True,
            "passed": bool(row_count > 0 and df["incident_id"].is_unique and df["incident_id"].notna().all()),
            "details": f"Unique incident_ids = {df['incident_id'].nunique()}/{row_count}"
        },
        {
            "name": "expect_column_values_to_not_be_null_core_keys",
            "critical": True,
            "passed": bool(row_count > 0 and df[["date_key", "comuna", "barrio", "clase_vehiculo", "hour_of_day"]].notna().all().all()),
            "details": "Null check across core dimensional foreign/business keys"
        },
        {
            "name": "expect_column_values_to_be_between_hour_of_day",
            "critical": True,
            "passed": bool(row_count > 0 and df["hour_of_day"].notna().all() and df["hour_of_day"].between(0, 23).all()),
            "details": f"Hour range: [{hour_min}, {hour_max}]"
        },
        {
            "name": "expect_column_values_to_be_between_temperature_c",
            "critical": True,
            "passed": bool(row_count > 0 and df["temperature_c"].notna().all() and df["temperature_c"].between(5.0, 45.0).all()),
            "details": f"Temp range: [{temp_min}, {temp_max}]"
        },
        {
            "name": "expect_column_values_to_be_between_traffic_density",
            "critical": True,
            "passed": bool(row_count > 0 and df["traffic_density_index"].notna().all() and df["traffic_density_index"].between(0.0, 1.0).all()),
            "details": f"Traffic density range: [{dens_min}, {dens_max}]"
        },
        {
            "name": "expect_column_values_to_be_in_set_target_is_severe_injury",
            "critical": True,
            "passed": bool(row_count > 0 and df["is_severe_injury"].notna().all() and set(df["is_severe_injury"].dropna().unique()).issubset({0, 1})),
            "details": f"Target classes present: {sorted(df['is_severe_injury'].dropna().unique().tolist())}"
        }
    ]

    total = len(expectations)
    passed_count = sum(1 for e in expectations if e["passed"])
    failed_count = total - passed_count
    overall_success = failed_count == 0

    report = {
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "suite_name": "cali_traffic_enriched_dq_suite",
        "success": overall_success,
        "evaluated_expectations": total,
        "successful_expectations": passed_count,
        "unsuccessful_expectations": failed_count,
        "results": expectations
    }

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