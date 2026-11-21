PRAGMA foreign_keys = ON;

-- 1. DIMENSIONES DEL MODELO ESTRELLA (STAR SCHEMA)

-- Dimensión Temporal (grano: fecha calendario)
CREATE TABLE IF NOT EXISTS dim_tiempo (
    tiempo_key INTEGER PRIMARY KEY AUTOINCREMENT,
    fecha_hecho TEXT NOT NULL UNIQUE,
    anio INTEGER NOT NULL CHECK (anio BETWEEN 2000 AND 2100),
    mes INTEGER NOT NULL CHECK (mes BETWEEN 1 AND 12),
    nombre_mes TEXT NOT NULL,
    trimestre INTEGER NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
    dia_mes INTEGER NOT NULL CHECK (dia_mes BETWEEN 1 AND 31),
    dia_semana INTEGER NOT NULL CHECK (dia_semana BETWEEN 0 AND 6),
    nombre_dia TEXT NOT NULL,
    es_fin_de_semana INTEGER NOT NULL CHECK (es_fin_de_semana IN (0, 1))
);

-- Dimensión Espacial / Infraestructura Vial (Fuente 1 + Fuente 2 respaldo)
CREATE TABLE IF NOT EXISTS dim_ubicacion (
    ubicacion_key INTEGER PRIMARY KEY AUTOINCREMENT,
    comuna TEXT NOT NULL,
    barrio TEXT NOT NULL,
    zona TEXT NOT NULL DEFAULT 'Urbana General',
    via_tipo TEXT NOT NULL DEFAULT 'Avenida Principal',
    traffic_density_index REAL NOT NULL DEFAULT 0.65 CHECK (traffic_density_index BETWEEN 0.0 AND 1.0),
    UNIQUE (comuna, barrio, via_tipo)
);

-- Dimensión de Clasificación del Siniestro y Vehículos Implicados
CREATE TABLE IF NOT EXISTS dim_vehiculo_reporte (
    vehiculo_reporte_key INTEGER PRIMARY KEY AUTOINCREMENT,
    clase_accidente TEXT NOT NULL,
    vehiculos_implicados TEXT NOT NULL,
    medio_reporte TEXT NOT NULL,
    UNIQUE (clase_accidente, vehiculos_implicados, medio_reporte)
);

-- Dimensión Meteorológica Horaria (Fuente 3: Open-Meteo Public API)
CREATE TABLE IF NOT EXISTS dim_clima (
    clima_key INTEGER PRIMARY KEY AUTOINCREMENT,
    hour_of_day INTEGER NOT NULL UNIQUE CHECK (hour_of_day BETWEEN 0 AND 23),
    franja_horaria TEXT NOT NULL,
    temperature_c REAL NOT NULL,
    precipitation_mm REAL NOT NULL CHECK (precipitation_mm >= 0.0),
    wind_speed_kmh REAL NOT NULL CHECK (wind_speed_kmh >= 0.0),
    condicion_lluvia TEXT NOT NULL
);

-- Dimensión Histórica de Homicidios por Condición de Víctima (Fuente 2 real: 2004-2021)
CREATE TABLE IF NOT EXISTS dim_victimas_historico (
    victima_hist_key INTEGER PRIMARY KEY AUTOINCREMENT,
    condicion_victima TEXT NOT NULL UNIQUE,
    total_2004_2021 INTEGER NOT NULL CHECK (total_2004_2021 >= 0),
    participacion_pct REAL NOT NULL CHECK (participacion_pct >= 0.0 AND participacion_pct <= 100.0),
    fuente_origen TEXT NOT NULL DEFAULT 'homicidios_santiago_cali_2004_2021.csv'
);

-- 2. TABLA DE HECHOS PRINCIPAL (BATCH ETL - 371,003 REGISTROS LIMPIOS)

CREATE TABLE IF NOT EXISTS fact_siniestros (
    siniestro_sk INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_id TEXT NOT NULL UNIQUE,
    tiempo_key INTEGER NOT NULL,
    ubicacion_key INTEGER NOT NULL,
    vehiculo_reporte_key INTEGER NOT NULL,
    clima_key INTEGER NOT NULL,
    fecha_hecho TEXT NOT NULL,
    hora_hecho INTEGER NOT NULL CHECK (hora_hecho BETWEEN 0 AND 23),
    direccion_reporte TEXT NOT NULL DEFAULT 'Sin dirección',
    numero_vehiculos REAL CHECK (numero_vehiculos IS NULL OR (numero_vehiculos >= 1 AND numero_vehiculos <= 30)),
    tipo_confirmado_limpio TEXT NOT NULL,
    es_grave INTEGER NOT NULL CHECK (es_grave IN (0, 1)),
    fecha_carga_etl TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (tiempo_key) REFERENCES dim_tiempo(tiempo_key),
    FOREIGN KEY (ubicacion_key) REFERENCES dim_ubicacion(ubicacion_key),
    FOREIGN KEY (vehiculo_reporte_key) REFERENCES dim_vehiculo_reporte(vehiculo_reporte_key),
    FOREIGN KEY (clima_key) REFERENCES dim_clima(clima_key)
);

-- 3. TABLA OPERATIVA DE PREDICCIONES EN STREAMING (KAFKA CONSUMER)

CREATE TABLE IF NOT EXISTS fact_predicciones_streaming (
    prediccion_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    incident_id TEXT,
    event_timestamp TEXT NOT NULL,
    clase_accidente TEXT NOT NULL,
    vehiculos_implicados TEXT NOT NULL,
    comuna TEXT NOT NULL,
    hora_hecho INTEGER NOT NULL,
    precipitation_mm REAL NOT NULL,
    traffic_density_index REAL NOT NULL,
    prediccion_es_grave INTEGER NOT NULL CHECK (prediccion_es_grave IN (0, 1)),
    probabilidad_grave REAL NOT NULL CHECK (probabilidad_grave BETWEEN 0.0 AND 1.0),
    nivel_alerta TEXT NOT NULL,
    processed_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 4. ÍNDICES DE RENDIMIENTO PARA CONSULTAS ANALÍTICAS Y FILTRADO

CREATE INDEX IF NOT EXISTS idx_fact_siniestros_fecha ON fact_siniestros(fecha_hecho);
CREATE INDEX IF NOT EXISTS idx_fact_siniestros_tiempo_key ON fact_siniestros(tiempo_key);
CREATE INDEX IF NOT EXISTS idx_fact_siniestros_ubicacion_key ON fact_siniestros(ubicacion_key);
CREATE INDEX IF NOT EXISTS idx_fact_siniestros_vehiculo_key ON fact_siniestros(vehiculo_reporte_key);
CREATE INDEX IF NOT EXISTS idx_fact_siniestros_clima_key ON fact_siniestros(clima_key);
CREATE INDEX IF NOT EXISTS idx_fact_siniestros_es_grave ON fact_siniestros(es_grave);
CREATE INDEX IF NOT EXISTS idx_dim_tiempo_anio_mes ON dim_tiempo(anio, mes);
CREATE INDEX IF NOT EXISTS idx_streaming_processed_at ON fact_predicciones_streaming(processed_at);

-- 5. VISTAS ANALÍTICAS PARA REPORTE ESTÁTICO Y CONSUMO BI

CREATE VIEW IF NOT EXISTS vw_siniestros_analitico AS
SELECT
    f.incident_id,
    t.fecha_hecho,
    t.anio,
    t.mes,
    t.nombre_mes,
    t.trimestre,
    t.nombre_dia,
    t.es_fin_de_semana,
    f.hora_hecho,
    c.franja_horaria,
    u.comuna,
    u.barrio,
    u.zona,
    u.via_tipo,
    u.traffic_density_index,
    f.direccion_reporte,
    v.clase_accidente,
    v.vehiculos_implicados,
    v.medio_reporte,
    f.numero_vehiculos,
    c.temperature_c,
    c.precipitation_mm,
    c.wind_speed_kmh,
    c.condicion_lluvia,
    f.tipo_confirmado_limpio,
    f.es_grave
FROM fact_siniestros f
INNER JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
INNER JOIN dim_ubicacion u ON f.ubicacion_key = u.ubicacion_key
INNER JOIN dim_vehiculo_reporte v ON f.vehiculo_reporte_key = v.vehiculo_reporte_key
INNER JOIN dim_clima c ON f.clima_key = c.clima_key;

CREATE VIEW IF NOT EXISTS vw_resumen_anual_gravedad AS
SELECT
    t.anio,
    COUNT(*) AS total_siniestros,
    SUM(f.es_grave) AS total_graves,
    SUM(CASE WHEN f.es_grave = 0 THEN 1 ELSE 0 END) AS total_no_graves,
    ROUND(100.0 * SUM(f.es_grave) / COUNT(*), 2) AS tasa_gravedad_pct,
    ROUND(AVG(f.numero_vehiculos), 2) AS promedio_vehiculos
FROM fact_siniestros f
INNER JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
GROUP BY t.anio
ORDER BY t.anio;

CREATE VIEW IF NOT EXISTS vw_riesgo_por_vehiculo_clima AS
SELECT
    v.vehiculos_implicados,
    v.clase_accidente,
    c.condicion_lluvia,
    COUNT(*) AS total_eventos,
    SUM(f.es_grave) AS eventos_graves,
    ROUND(100.0 * SUM(f.es_grave) / COUNT(*), 2) AS tasa_gravedad_pct
FROM fact_siniestros f
INNER JOIN dim_vehiculo_reporte v ON f.vehiculo_reporte_key = v.vehiculo_reporte_key
INNER JOIN dim_clima c ON f.clima_key = c.clima_key
GROUP BY v.vehiculos_implicados, v.clase_accidente, c.condicion_lluvia
HAVING COUNT(*) >= 10
ORDER BY tasa_gravedad_pct DESC, total_eventos DESC;