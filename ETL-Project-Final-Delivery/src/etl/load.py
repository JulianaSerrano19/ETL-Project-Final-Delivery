import logging
import sqlite3
from pathlib import Path
import pandas as pd

from src.utils.config import BASE_DIR, DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SCHEMA_PATH = BASE_DIR / "database" / "schema.sql"

MESES_ES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre",
}

DIAS_ES = {
    0: "Lunes", 1: "Martes", 2: "Miércoles", 3: "Jueves",
    4: "Viernes", 5: "Sábado", 6: "Domingo",
}


def _franja_horaria(h: int) -> str:
    if 0 <= h <= 5:
        return "Madrugada (00-05)"
    if 6 <= h <= 11:
        return "Mañana (06-11)"
    if 12 <= h <= 17:
        return "Tarde (12-17)"
    return "Noche (18-23)"


def load_to_warehouse(
    df_enriched: pd.DataFrame,
    df_homicides_hist: pd.DataFrame | None = None,
    db_path: Path = DB_PATH,
) -> None:
    """
    Ejecuta el DDL de database/schema.sql y carga de manera idempotente
    las dimensiones y la tabla de hechos en SQLite.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Loading Star Schema into SQLite warehouse at {db_path}...")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
            conn.executescript(f.read())

        # Limpiar tablas batch para carga idempotente
        conn.execute("DELETE FROM fact_siniestros;")
        conn.execute("DELETE FROM dim_tiempo;")
        conn.execute("DELETE FROM dim_ubicacion;")
        conn.execute("DELETE FROM dim_vehiculo_reporte;")
        conn.execute("DELETE FROM dim_clima;")

        # 1. Poblar dim_tiempo
        df_t = (
            df_enriched[["fecha_hecho", "anio", "mes", "dia_semana", "es_fin_de_semana"]]
            .drop_duplicates(subset=["fecha_hecho"])
            .copy()
        )
        dt_series = pd.to_datetime(df_t["fecha_hecho"])
        df_t["nombre_mes"] = df_t["mes"].map(MESES_ES)
        df_t["trimestre"] = dt_series.dt.quarter.astype(int)
        df_t["dia_mes"] = dt_series.dt.day.astype(int)
        df_t["nombre_dia"] = df_t["dia_semana"].map(DIAS_ES)
        df_t.to_sql("dim_tiempo", conn, if_exists="append", index=False)

        # 2. Poblar dim_ubicacion
        df_u = (
            df_enriched[["comuna", "barrio", "zona", "via_tipo", "traffic_density_index"]]
            .drop_duplicates(subset=["comuna", "barrio", "via_tipo"])
            .copy()
        )
        df_u.to_sql("dim_ubicacion", conn, if_exists="append", index=False)

        # 3. Poblar dim_vehiculo_reporte
        df_v = (
            df_enriched[["clase_accidente", "vehiculos_implicados", "medio_reporte"]]
            .drop_duplicates(subset=["clase_accidente", "vehiculos_implicados", "medio_reporte"])
            .copy()
        )
        df_v.to_sql("dim_vehiculo_reporte", conn, if_exists="append", index=False)

        # 4. Poblar dim_clima
        df_c = (
            df_enriched[["hora_hecho", "temperature_c", "precipitation_mm", "wind_speed_kmh"]]
            .drop_duplicates(subset=["hora_hecho"])
            .rename(columns={"hora_hecho": "hour_of_day"})
            .copy()
        )
        df_c["franja_horaria"] = df_c["hour_of_day"].apply(_franja_horaria)
        df_c["condicion_lluvia"] = df_c["precipitation_mm"].apply(
            lambda p: "Con lluvia" if p >= 0.5 else "Seco / Llovizna leve"
        )
        df_c.to_sql("dim_clima", conn, if_exists="append", index=False)

        # 5. Poblar dim_victimas_historico (si está disponible el CSV de homicidios 2004-2021)
        if df_homicides_hist is not None and not df_homicides_hist.empty:
            conn.execute("DELETE FROM dim_victimas_historico;")
            df_h = df_homicides_hist[
                df_homicides_hist["CONDICION_VICTIMA"].astype(str).str.upper() != "TOTAL"
            ].copy()
            df_h["total_2004_2021"] = pd.to_numeric(df_h["TOTAL"], errors="coerce").fillna(0).astype(int)
            total_general = max(int(df_h["total_2004_2021"].sum()), 1)
            df_h_out = pd.DataFrame({
                "condicion_victima": df_h["CONDICION_VICTIMA"].astype(str).str.strip(),
                "total_2004_2021": df_h["total_2004_2021"],
                "participacion_pct": (df_h["total_2004_2021"] / total_general * 100.0).round(2),
            })
            df_h_out.to_sql("dim_victimas_historico", conn, if_exists="append", index=False)

        # 6. Resolver llaves subrogadas y cargar fact_siniestros
        map_t = pd.read_sql("SELECT tiempo_key, fecha_hecho FROM dim_tiempo", conn)
        map_u = pd.read_sql("SELECT ubicacion_key, comuna, barrio, via_tipo FROM dim_ubicacion", conn)
        map_v = pd.read_sql(
            "SELECT vehiculo_reporte_key, clase_accidente, vehiculos_implicados, medio_reporte FROM dim_vehiculo_reporte",
            conn,
        )
        map_c = pd.read_sql("SELECT clima_key, hour_of_day FROM dim_clima", conn)

        fact = df_enriched.merge(map_t, on="fecha_hecho", how="inner")
        fact = fact.merge(map_u, on=["comuna", "barrio", "via_tipo"], how="inner")
        fact = fact.merge(map_v, on=["clase_accidente", "vehiculos_implicados", "medio_reporte"], how="inner")
        fact = fact.merge(map_c, left_on="hora_hecho", right_on="hour_of_day", how="inner")

        fact_out = fact[[
            "incident_id",
            "tiempo_key",
            "ubicacion_key",
            "vehiculo_reporte_key",
            "clima_key",
            "fecha_hecho",
            "hora_hecho",
            "direccion_reporte",
            "numero_vehiculos",
            "tipo_confirmado_limpio",
            "es_grave",
        ]].copy()

        fact_out.to_sql("fact_siniestros", conn, if_exists="append", index=False, chunksize=10000)
        conn.commit()
        logger.info(f"Warehouse load completed successfully. fact_siniestros rows={len(fact_out)}.")
    finally:
        conn.close()