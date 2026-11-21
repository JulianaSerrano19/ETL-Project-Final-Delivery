import logging
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests

from src.utils.config import (
    BASE_DIR,
    CALI_LAT,
    CALI_LON,
    DATA_RAW_DIR,
    OPEN_METEO_API_URL,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

REAL_SRC1_CANDIDATES = [
    DATA_RAW_DIR / "siniestralidad_2016_2024.csv",
    BASE_DIR / "siniestralidad_2016_2024.csv",
]

REAL_SRC2_CANDIDATES = [
    DATA_RAW_DIR / "homicidios_en_accidentes_de_transito_segun_condicion_de_victima_en_el_municipio_de_santiago_de_2.csv",
    BASE_DIR / "homicidios_en_accidentes_de_transito_segun_condicion_de_victima_en_el_municipio_de_santiago_de_2.csv",
]


def _find_existing_file(candidates: list[Path]) -> Path | None:
    for path in candidates:
        if path.exists() and path.is_file():
            return path
    return None


def _generate_fallback_source_1(src1_path: Path) -> pd.DataFrame:
    logger.warning("[Source 1] Generating reproducible synthetic baseline fallback...")
    rng = np.random.default_rng(42)
    n = 1500
    base_date = datetime(2025, 1, 1)
    dates = [base_date + timedelta(days=int(d)) for d in rng.integers(0, 365, size=n)]
    hours = rng.integers(0, 24, size=n)
    comunas = rng.choice(["Comuna 2", "Comuna 3", "Comuna 10", "Comuna 17", "Comuna 19", "Comuna 22"], size=n)
    barrios = rng.choice(["Granada", "San Fernando", "Ciudad Jardín", "Pance", "El Ingenio", "Centro"], size=n)
    vehiculos = rng.choice(
        ["Motocicleta", "Automóvil", "Camioneta", "Bus Urbano", "Bicicleta"],
        p=[0.48, 0.30, 0.10, 0.07, 0.05],
        size=n,
    )
    servicios = rng.choice(["Particular", "Público", "Oficial"], p=[0.80, 0.17, 0.03], size=n)

    df1 = pd.DataFrame({
        "incident_id": [f"CALI-{20250000 + i}" for i in range(n)],
        "fecha_hecho": [d.strftime("%Y-%m-%d") for d in dates],
        "hora_hecho": hours,
        "comuna": comunas,
        "barrio": barrios,
        "clase_vehiculo": vehiculos,
        "servicio": servicios,
        "victimas": rng.choice([0, 1, 2, 3], p=[0.45, 0.38, 0.13, 0.04], size=n),
    })
    src1_path.parent.mkdir(parents=True, exist_ok=True)
    df1.to_csv(src1_path, index=False)
    return df1


def _generate_fallback_source_2(src2_path: Path) -> pd.DataFrame:
    logger.warning("[Source 2] Generating reproducible synthetic baseline fallback...")
    rng = np.random.default_rng(99)
    comunas = ["Comuna 2", "Comuna 3", "Comuna 10", "Comuna 17", "Comuna 19", "Comuna 22"]
    barrios = ["Granada", "San Fernando", "Ciudad Jardín", "Pance", "El Ingenio", "Centro"]
    rows = []
    for c in comunas:
        for b in barrios:
            rows.append({
                "comuna": c,
                "barrio": b,
                "zona": "Sur" if c in ["Comuna 17", "Comuna 19", "Comuna 22"] else "Centro-Norte",
                "via_tipo": rng.choice(["Autopista", "Avenida Principal", "Calle Secundaria"]),
                "traffic_density_index": round(float(rng.uniform(0.35, 0.95)), 2),
            })
    df2 = pd.DataFrame(rows)
    src2_path.parent.mkdir(parents=True, exist_ok=True)
    df2.to_csv(src2_path, index=False)
    return df2


def ensure_reproducible_sources() -> tuple[Path, Path]:
    DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
    src1_fallback = DATA_RAW_DIR / "cali_accidents_primary.csv"
    src2_fallback = DATA_RAW_DIR / "cali_road_traffic_secondary.csv"

    if not src1_fallback.exists():
        _generate_fallback_source_1(src1_fallback)
    if not src2_fallback.exists():
        _generate_fallback_source_2(src2_fallback)

    return src1_fallback, src2_fallback


def extract_source_1_accidents() -> pd.DataFrame:
    """
    Lee el CSV real siniestralidad_2016_2024.csv (latin1, ';') si existe en data/raw/;
    de lo contrario usa el respaldo reproducible.
    """
    real_path = _find_existing_file(REAL_SRC1_CANDIDATES)
    src1_fallback, _ = ensure_reproducible_sources()

    if real_path is not None:
        df_raw = pd.read_csv(real_path, encoding="latin1", sep=";", low_memory=False)
        logger.info(f"[Source 1] Loaded real dataset '{real_path.name}' ({len(df_raw)} rows).")
        return df_raw

    logger.info(f"[Source 1] Real dataset not found; reading reproducible source '{src1_fallback.name}'.")
    return pd.read_csv(src1_fallback)


def extract_source_2_infrastructure() -> pd.DataFrame:
    """
    Verifica el CSV real de homicidios por condición de víctima; si no contiene
    claves espaciales (comuna, barrio, traffic_density_index), usa el respaldo
    reproducible para el cruce espacial y conserva el histórico para dimensión.
    """
    real_path = _find_existing_file(REAL_SRC2_CANDIDATES)
    _, src2_fallback = ensure_reproducible_sources()

    if real_path is not None:
        df_hom = pd.read_csv(real_path, encoding="utf-8")
        required_spatial_cols = {"comuna", "barrio", "traffic_density_index"}
        if required_spatial_cols.issubset(set(df_hom.columns)):
            logger.info(f"[Source 2] Loaded spatial dataset '{real_path.name}' ({len(df_hom)} rows).")
            return df_hom
        logger.warning(
            f"[Source 2] '{real_path.name}' ({len(df_hom)} rows) is an annual aggregated victim series "
            f"without spatial keys {sorted(required_spatial_cols)}; using reproducible spatial fallback."
        )

    logger.info(f"[Source 2] Reading reproducible source '{src2_fallback.name}'.")
    return pd.read_csv(src2_fallback)


def extract_source_2_homicides_history() -> pd.DataFrame | None:
    """
    Extrae el histórico anual de homicidios (2004-2021) si el CSV real está en data/raw/,
    para cargarlo en dim_victimas sin mezclar granularidad con fact_siniestros.
    """
    real_path = _find_existing_file(REAL_SRC2_CANDIDATES)
    if real_path is None:
        return None
    df_hom = pd.read_csv(real_path, encoding="utf-8")
    logger.info(f"[Source 2 - History] Loaded '{real_path.name}' ({len(df_hom)} rows).")
    return df_hom


def extract_source_3_weather_api() -> pd.DataFrame:
    params = {
        "latitude": CALI_LAT,
        "longitude": CALI_LON,
        "hourly": "temperature_2m,precipitation,wind_speed_10m",
        "past_days": 7,
        "forecast_days": 1,
        "timezone": "America/Bogota",
    }
    try:
        resp = requests.get(OPEN_METEO_API_URL, params=params, timeout=10)
        resp.raise_for_status()
        payload = resp.json()
        hourly = payload.get("hourly", {})
        df_weather = pd.DataFrame({
            "timestamp": hourly["time"],
            "temperature_c": hourly["temperature_2m"],
            "precipitation_mm": hourly["precipitation"],
            "wind_speed_kmh": hourly["wind_speed_10m"],
        })
        df_weather["hour_of_day"] = pd.to_datetime(df_weather["timestamp"]).dt.hour
        df_agg = df_weather.groupby("hour_of_day", as_index=False).agg({
            "temperature_c": "mean",
            "precipitation_mm": "mean",
            "wind_speed_kmh": "mean",
        }).round(2)
        logger.info(f"[Source 3 - Public API] Extracted {len(df_agg)} hourly weather profiles from Open-Meteo.")
        return df_agg
    except Exception as e:
        logger.warning(f"[Source 3 - Public API] Unavailable ({e}); using reproducible fallback.")
        hours = list(range(24))
        return pd.DataFrame({
            "hour_of_day": hours,
            "temperature_c": [21.0 + 7.5 * np.sin((h - 6) * np.pi / 12) if 6 <= h <= 18 else 20.5 for h in hours],
            "precipitation_mm": [1.8 if h in [15, 16, 17, 18, 19] else 0.1 for h in hours],
            "wind_speed_kmh": [12.5 if 13 <= h <= 18 else 6.0 for h in hours],
        }).round(2)