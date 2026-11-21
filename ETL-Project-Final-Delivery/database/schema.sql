-- Dimensional Model (Star Schema) for Cali Traffic Incidents & Weather Enrichment
-- Grain of fact_traffic_incident: One recorded traffic incident event in Cali, Colombia.

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY, -- YYYYMMDD
    full_date TEXT NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name TEXT NOT NULL,
    is_weekend INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_location (
    location_key INTEGER PRIMARY KEY AUTOINCREMENT,
    comuna TEXT NOT NULL,
    barrio TEXT NOT NULL,
    zona TEXT NOT NULL,
    via_tipo TEXT NOT NULL,
    UNIQUE(comuna, barrio, zona, via_tipo)
);

CREATE TABLE IF NOT EXISTS dim_vehicle (
    vehicle_key INTEGER PRIMARY KEY AUTOINCREMENT,
    clase_vehiculo TEXT NOT NULL,
    servicio TEXT NOT NULL,
    UNIQUE(clase_vehiculo, servicio)
);

CREATE TABLE IF NOT EXISTS dim_weather (
    weather_key INTEGER PRIMARY KEY AUTOINCREMENT,
    weather_condition TEXT NOT NULL,
    temp_range TEXT NOT NULL,
    precipitation_flag INTEGER NOT NULL,
    UNIQUE(weather_condition, temp_range, precipitation_flag)
);

CREATE TABLE IF NOT EXISTS fact_traffic_incident (
    incident_id TEXT PRIMARY KEY, -- Natural key for idempotent upserts
    date_key INTEGER NOT NULL,
    location_key INTEGER NOT NULL,
    vehicle_key INTEGER NOT NULL,
    weather_key INTEGER NOT NULL,
    hour_of_day INTEGER NOT NULL,
    temperature_c REAL NOT NULL,
    precipitation_mm REAL NOT NULL,
    wind_speed_kmh REAL NOT NULL,
    traffic_density_index REAL NOT NULL,
    victims_count INTEGER NOT NULL,
    is_severe_injury INTEGER NOT NULL, -- Target variable (1 = Severe/Fatal, 0 = Property Damage/Minor)
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (location_key) REFERENCES dim_location(location_key),
    FOREIGN KEY (vehicle_key) REFERENCES dim_vehicle(vehicle_key),
    FOREIGN KEY (weather_key) REFERENCES dim_weather(weather_key)
);

-- Operational Streaming Predictions Table (Populated by Kafka Consumer)
CREATE TABLE IF NOT EXISTS streaming_incident_predictions (
    prediction_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT UNIQUE NOT NULL,
    incident_id TEXT NOT NULL,
    event_timestamp TEXT NOT NULL,
    comuna TEXT,
    clase_vehiculo TEXT,
    hour_of_day INTEGER,
    temperature_c REAL,
    precipitation_mm REAL,
    traffic_density_index REAL,
    predicted_severe_risk INTEGER NOT NULL,
    severe_probability REAL NOT NULL,
    risk_level TEXT NOT NULL,
    processed_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Data Quality Audit Log Table
CREATE TABLE IF NOT EXISTS dq_validation_logs (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_timestamp TEXT NOT NULL,
    suite_name TEXT NOT NULL,
    success INTEGER NOT NULL,
    evaluated_expectations INTEGER NOT NULL,
    successful_expectations INTEGER NOT NULL,
    unsuccessful_expectations INTEGER NOT NULL,
    details TEXT
);
