import logging
import numpy as np
import pandas as pd

from src.utils.config import DATA_PROCESSED_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

MISSING_TOKENS = {"", ".", "na", "n/a", "null", "none", "nan", "sd", "sin dato"}

ML_FEATURE_COLUMNS = [
    "anio",
    "mes",
    "dia_semana",
    "es_fin_de_semana",
    "hora_hecho",
    "clase_accidente",
    "medio_reporte",
    "vehiculos_implicados",
    "numero_vehiculos",
    "comuna",
    "zona",
    "via_tipo",
    "traffic_density_index",
    "temperature_c",
    "precipitation_mm",
    "wind_speed_kmh",
]
ML_TARGET_COLUMN = "es_grave"


def _clean_text_series(s: pd.Series, default: str = "No reportado") -> pd.Series:
    cleaned = (
        s.astype(str)
        .str.replace("TELEF?NICO", "TELEFÓNICO", regex=False)
        .str.replace("Ca?da", "Caída", regex=False)
        .str.replace("Da?os", "Daños", regex=False)
        .str.replace("Intervenci?n", "Intervención", regex=False)
        .str.strip()
    )
    mask_missing = cleaned.str.lower().isin(MISSING_TOKENS) | s.isna()
    return cleaned.where(~mask_missing, default)


def _normalize_confirmado(s: pd.Series) -> pd.Series:
    base = _clean_text_series(s, default="No reportado").str.lower()
    mapping = {
        "negativo": "Negativo",
        "solo daños": "Solo daños",
        "con lesionado": "Con lesionado",
        "con heridos": "Con lesionado",
        "choque con lesionados": "Con lesionado",
        "con fallecido": "Con fallecido",
        "con fallecido (foraneo)": "Con fallecido (Foraneo)",
        "no reportado": "No reportado",
    }
    return base.map(mapping).fillna(base.str.title())


def _normalize_clase_accidente(s: pd.Series) -> pd.Series:
    base = _clean_text_series(s, default="No reportado").str.lower()
    mapping = {
        "choque": "Choque",
        "atropello": "Atropello",
        "volcamiento": "Volcamiento",
        "caida de ocupante": "Caída de ocupante",
        "caída de ocupante": "Caída de ocupante",
        "caída ocupante": "Caída de ocupante",
        "incendio": "Incendio",
        "no aplica": "No aplica",
        "no reportado": "No reportado",
    }
    return base.map(mapping).fillna(base.str.title())


def transform_and_integrate(
    df_accidents: pd.DataFrame,
    df_infra: pd.DataFrame,
    df_weather: pd.DataFrame,
) -> pd.DataFrame:
    """
    Transforma y unifica las 3 fuentes conservando todos los registros válidos
    de fact_siniestros (371,003 en fuente real) y definiendo el target `es_grave`.
    """
    logger.info("Starting transformation and multi-source integration...")
    df1 = df_accidents.loc[:, ~df_accidents.columns.duplicated()].copy()

    rename_map = {
        "Código": "incident_id",
        "código": "incident_id",
        "Fecha": "fecha_hecho",
        "fecha": "fecha_hecho",
        "Tipo confirmado": "tipo_confirmado",
        "Dirección reporte": "direccion_reporte",
        "dirección_reporte": "direccion_reporte",
        "Tipo clase de accidente": "clase_accidente",
        "tipo_clase_de_accidente": "clase_accidente",
        "Medio de reporte": "medio_reporte",
        "medio_de_reporte": "medio_reporte",
        "Numero vehículos": "numero_vehiculos",
        "numero_vehículos": "numero_vehiculos",
        "Tipo de vehículos implicados": "vehiculos_implicados",
        "tipo_de_vehículos_implicados": "vehiculos_implicados",
        "clase_vehiculo": "vehiculos_implicados",
        "servicio": "medio_reporte",
    }
    df1 = df1.rename(columns={k: v for k, v in rename_map.items() if k in df1.columns})

    # Identificador único
    if "incident_id" in df1.columns:
        df1["incident_id"] = _clean_text_series(df1["incident_id"], default="")
        df1 = df1[df1["incident_id"] != ""].drop_duplicates(subset=["incident_id"], keep="first").copy()

    # Fechas y variables temporales
    parsed_date = pd.to_datetime(df1["fecha_hecho"], errors="coerce", dayfirst=True)
    df1 = df1[parsed_date.notna()].copy()
    parsed_date = parsed_date.loc[df1.index]
    df1["fecha_dt"] = parsed_date
    df1["fecha_hecho"] = parsed_date.dt.strftime("%Y-%m-%d")
    df1["anio"] = parsed_date.dt.year.astype(int)
    df1["mes"] = parsed_date.dt.month.astype(int)
    df1["dia_semana"] = parsed_date.dt.dayofweek.astype(int)
    df1["es_fin_de_semana"] = df1["dia_semana"].isin([5, 6]).astype(int)

    if "hora_hecho" not in df1.columns:
        df1["hora_hecho"] = 12
    else:
        df1["hora_hecho"] = pd.to_numeric(df1["hora_hecho"], errors="coerce").fillna(12).astype(int).clip(0, 23)

    # Target binario: es_grave (1 = lesionado o fallecido, 0 = resto)
    if "tipo_confirmado" in df1.columns:
        df1["tipo_confirmado_limpio"] = _normalize_confirmado(df1["tipo_confirmado"])
        df1[ML_TARGET_COLUMN] = (
            df1["tipo_confirmado_limpio"]
            .isin(["Con lesionado", "Con fallecido", "Con fallecido (Foraneo)"])
            .astype(int)
        )
    elif "victimas" in df1.columns:
        vict = pd.to_numeric(df1["victimas"], errors="coerce").fillna(0)
        df1["tipo_confirmado_limpio"] = np.where(vict >= 1, "Con lesionado", "Solo daños")
        df1[ML_TARGET_COLUMN] = (vict >= 1).astype(int)
    else:
        df1["tipo_confirmado_limpio"] = "No reportado"
        df1[ML_TARGET_COLUMN] = 0

    # Estandarización categórica
    if "clase_accidente" in df1.columns:
        df1["clase_accidente"] = _normalize_clase_accidente(df1["clase_accidente"])
    else:
        df1["clase_accidente"] = "No reportado"

    for col, default_val in [
        ("direccion_reporte", "Sin dirección"),
        ("medio_reporte", "No reportado"),
        ("vehiculos_implicados", "No reportado"),
        ("comuna", "Sin comuna en fuente"),
        ("barrio", "Sin barrio en fuente"),
    ]:
        if col in df1.columns:
            df1[col] = _clean_text_series(df1[col], default=default_val)
        else:
            df1[col] = default_val

    if "numero_vehiculos" in df1.columns:
        num_veh = pd.to_numeric(df1["numero_vehiculos"], errors="coerce")
        df1["numero_vehiculos"] = num_veh.where((num_veh >= 1) & (num_veh <= 30), np.nan)
    else:
        df1["numero_vehiculos"] = np.nan

    # Merge 1 + 2 (espacial si hay claves coincidentes)
    merged = df1
    if df_infra is not None and not df_infra.empty:
        df2 = df_infra.loc[:, ~df_infra.columns.duplicated()].copy()
        join_keys = [k for k in ["comuna", "barrio"] if k in merged.columns and k in df2.columns]
        if join_keys:
            df2 = df2.drop_duplicates(subset=join_keys, keep="first")
            merged = pd.merge(merged, df2, on=join_keys, how="left")

    merged["zona"] = merged["zona"].fillna("Urbana General") if "zona" in merged.columns else "Urbana General"
    merged["via_tipo"] = merged["via_tipo"].fillna("Avenida Principal") if "via_tipo" in merged.columns else "Avenida Principal"
    if "traffic_density_index" in merged.columns:
        med = merged["traffic_density_index"].median()
        merged["traffic_density_index"] = merged["traffic_density_index"].fillna(0.65 if pd.isna(med) else med)
    else:
        merged["traffic_density_index"] = 0.65

    # Merge + 3 (clima horario Open-Meteo)
    if df_weather is not None and not df_weather.empty and "hour_of_day" in df_weather.columns:
        df3 = df_weather.drop_duplicates(subset=["hour_of_day"], keep="first").copy()
        df3["hour_of_day"] = pd.to_numeric(df3["hour_of_day"], errors="coerce").astype("Int64")
        merged = pd.merge(merged, df3, left_on="hora_hecho", right_on="hour_of_day", how="left")

    for w_col, fill_val in [("temperature_c", 24.0), ("precipitation_mm", 0.0), ("wind_speed_kmh", 8.0)]:
        if w_col in merged.columns:
            merged[w_col] = pd.to_numeric(merged[w_col], errors="coerce").fillna(fill_val)
        else:
            merged[w_col] = fill_val

    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    out_parquet = DATA_PROCESSED_DIR / "cali_mobility_enriched.parquet"
    merged.to_parquet(out_parquet, index=False)

    logger.info(
        f"Transformation complete. Rows={len(merged)}, "
        f"es_grave=1: {int(merged[ML_TARGET_COLUMN].sum())} ({merged[ML_TARGET_COLUMN].mean() * 100:.2f}%)."
    )
    return merged