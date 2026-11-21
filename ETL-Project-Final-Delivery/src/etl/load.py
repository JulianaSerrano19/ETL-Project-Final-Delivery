import sqlite3
import logging
from pathlib import Path
import pandas as pd
from src.utils.config import DB_PATH, BASE_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def init_database():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    schema_path = BASE_DIR / "database" / "schema.sql"
    with open(schema_path, "r", encoding="utf-8") as f:
        schema_sql = f.read()
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript(schema_sql)
        conn.commit()
    logger.info(f"Initialized database schema at {DB_PATH}")


def load_dimensional_model(df: pd.DataFrame):
    """
    Loads validated data into the Star Schema (dim_date, dim_location, dim_vehicle, dim_weather, fact_traffic_incident).
    Idempotent: Uses INSERT OR IGNORE on dimensions and INSERT OR REPLACE on fact_traffic_incident keyed by incident_id.
    """
    init_database()
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()

        # 1. Load dim_date
        dates = df[["date_key", "full_date", "year", "month", "day", "day_of_week", "day_name", "is_weekend"]].drop_duplicates()
        cur.executemany(
            """INSERT OR IGNORE INTO dim_date
               (date_key, full_date, year, month, day, day_of_week, day_name, is_weekend)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            dates.itertuples(index=False, name=None)
        )

        # 2. Load dim_location
        locs = df[["comuna", "barrio", "zona", "via_tipo"]].drop_duplicates()
        cur.executemany(
            """INSERT OR IGNORE INTO dim_location (comuna, barrio, zona, via_tipo)
               VALUES (?, ?, ?, ?)""",
            locs.itertuples(index=False, name=None)
        )

        # 3. Load dim_vehicle
        vehs = df[["clase_vehiculo", "servicio"]].drop_duplicates()
        cur.executemany(
            """INSERT OR IGNORE INTO dim_vehicle (clase_vehiculo, servicio)
               VALUES (?, ?)""",
            vehs.itertuples(index=False, name=None)
        )

        # 4. Load dim_weather
        weathers = df[["weather_condition", "temp_range", "precipitation_flag"]].drop_duplicates()
        cur.executemany(
            """INSERT OR IGNORE INTO dim_weather (weather_condition, temp_range, precipitation_flag)
               VALUES (?, ?, ?)""",
            weathers.itertuples(index=False, name=None)
        )
        conn.commit()

        # Retrieve Surrogate Keys
        loc_map = {
            (r[1], r[2], r[3], r[4]): r[0]
            for r in cur.execute("SELECT location_key, comuna, barrio, zona, via_tipo FROM dim_location").fetchall()
        }
        veh_map = {
            (r[1], r[2]): r[0]
            for r in cur.execute("SELECT vehicle_key, clase_vehiculo, servicio FROM dim_vehicle").fetchall()
        }
        weather_map = {
            (r[1], r[2], r[3]): r[0]
            for r in cur.execute("SELECT weather_key, weather_condition, temp_range, precipitation_flag FROM dim_weather").fetchall()
        }

        # 5. Build & Upsert fact_traffic_incident
        fact_rows = []
        for row in df.itertuples(index=False):
            l_key = loc_map[(row.comuna, row.barrio, row.zona, row.via_tipo)]
            v_key = veh_map[(row.clase_vehiculo, row.servicio)]
            w_key = weather_map[(row.weather_condition, row.temp_range, int(row.precipitation_flag))]
            fact_rows.append((
                row.incident_id,
                int(row.date_key),
                l_key,
                v_key,
                w_key,
                int(row.hour_of_day),
                float(row.temperature_c),
                float(row.precipitation_mm),
                float(row.wind_speed_kmh),
                float(row.traffic_density_index),
                int(row.victims_count),
                int(row.is_severe_injury)
            ))

        cur.executemany(
            """INSERT OR REPLACE INTO fact_traffic_incident
               (incident_id, date_key, location_key, vehicle_key, weather_key,
                hour_of_day, temperature_c, precipitation_mm, wind_speed_kmh,
                traffic_density_index, victims_count, is_severe_injury)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            fact_rows
        )
        conn.commit()
        logger.info(f"Loaded/Upserted {len(fact_rows)} records into fact_traffic_incident.")
