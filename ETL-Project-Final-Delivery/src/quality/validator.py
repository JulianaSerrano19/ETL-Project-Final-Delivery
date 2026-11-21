import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def validate_transformed_data(df: pd.DataFrame) -> dict:
    """
    Ejecuta controles de calidad (completitud, unicidad, rangos y dominios)
    sobre el dataset unificado antes de la carga y entrenamiento ML.
    """
    checks = {}

    # 1. Dataset no vacío y columnas críticas presentes
    required_cols = {
        "incident_id",
        "fecha_hecho",
        "anio",
        "mes",
        "tipo_confirmado_limpio",
        "clase_accidente",
        "es_grave",
        "temperature_c",
    }
    checks["required_columns_present"] = required_cols.issubset(set(df.columns))
    checks["row_count_positive"] = len(df) > 0

    # 2. Unicidad y no-nulidad de clave primaria
    checks["incident_id_not_null"] = df["incident_id"].notna().all()
    checks["incident_id_unique"] = df["incident_id"].nunique() == len(df)

    # 3. Rango temporal válido (2016-2026)
    checks["valid_year_range"] = df["anio"].between(2016, 2026).all()
    checks["valid_month_range"] = df["mes"].between(1, 12).all()

    # 4. Target binario válido y sin nulos
    checks["target_binary_valid"] = set(df["es_grave"].dropna().unique()).issubset({0, 1})

    # 5. Sin marcadores crudos '.' en columnas limpias
    checks["no_raw_dots_in_clase"] = not (df["clase_accidente"] == ".").any()
    checks["no_raw_dots_in_confirmado"] = not (df["tipo_confirmado_limpio"] == ".").any()

    # 6. Rango plausible de numero_vehiculos (ignorando NaN)
    if "numero_vehiculos" in df.columns:
        valid_nv = df["numero_vehiculos"].dropna()
        checks["valid_vehicle_count_range"] = valid_nv.between(1, 30).all() if len(valid_nv) > 0 else True

    passed = all(checks.values())
    report = {
        "passed": passed,
        "total_rows": len(df),
        "positive_target_rate": round(float(df["es_grave"].mean()), 4) if "es_grave" in df.columns else None,
        "checks": checks,
    }

    if not passed:
        failed = [k for k, v in checks.items() if not v]
        logger.error(f"Data Quality Gate FAILED on checks: {failed}")
        raise ValueError(f"Data Quality validation failed: {failed}")

    logger.info(f"Data Quality Gate PASSED ({len(df)} rows validated).")
    return report