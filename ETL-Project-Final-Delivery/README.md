# ETL Project — Final Delivery: End-to-End Cali Mobility Data Warehouse & Real-Time Risk Streaming
# Realizado por: Laura Torres, Juliana Toro Serrano

Extension of **ETL-Project-Second-Delivery**g integrating multi-source batch orchestration (Apache Airflow + Great Expectations + Star Schema) with operational real-time machine learning inference (Apache Kafka + Scikit-Learn + Streamlit).

## Repository Structure
```text
ETL-Project-Final-Delivery/
├── airflow/dags/               # Airflow DAG orchestrating Extract -> Transform -> DQ Gate -> Load -> Kafka
├── dashboard/                  # Real-time Streamlit dashboard driven by Kafka consumer predictions
├── database/                   # Star Schema DDL (schema.sql) & SQLite warehouse
├── docs/                       # Technical documentation & architecture diagrams
├── great_expectations/         # Data quality expectation suite definitions
├── kafka/                      # Kafka Producer (fact-table streamer) & Consumer (ML inference engine)
├── ml/                         # Model training script & serialized .pkl pipeline artifact
├── notebooks/                  # Preserved EDA notebooks from Deliveries 1 & 2
├── reports/                    # Static analytical report generator & output charts
├── src/
│   ├── etl/                    # Modular extract, transform, load, and pipeline runner scripts
│   ├── quality/                # Great Expectations validation gate & audit logger
│   └── utils/                  # Centralized configuration
├── docker-compose.yml          # Kafka & Zookeeper local cluster
├── requirements.txt            # Python dependencies
└── .gitignore
```

## Quickstart & Execution Guide

### 1. Environment Setup
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
export PYTHONPATH=.        # On Windows PowerShell: $env:PYTHONPATH="."
```

### 2. Run Batch ETL Pipeline (Extract, Integrate, Validate, Load Star Schema)
You can trigger the Airflow DAG (`airflow/dags/cali_mobility_etl_dag.py`) or run the equivalent modular pipeline directly:
```bash
python src/etl/run_pipeline.py
```

### 3. Train & Serialize the Machine Learning Pipeline
```bash
python ml/train_model.py
```
This generates `ml/models/accident_severity_pipeline.pkl` containing both the fitted `ColumnTransformer` preprocessor and the `RandomForestClassifier`.

### 4. Generate the Static Analytical Report
```bash
python reports/generate_static_report.py
```
Outputs `reports/static_output/static_analytical_report.md` and PNG visualizations queried directly from the validated Star Schema.

### 5. Run Real-Time Kafka Streaming & Live Dashboard
1. Start Kafka & Zookeeper:
   ```bash
   docker-compose up -d
   ```
2. Start the Kafka Consumer (loads `.pkl` model and writes predictions):
   ```bash
   python kafka/consumer.py
   ```
3. In a second terminal, launch the Real-Time Streamlit Dashboard:
   ```bash
   streamlit run dashboard/app.py
   ```
4. In a third terminal, start the Kafka Producer to stream incident events from the fact table:
   ```bash
   python kafka/producer.py
   ```
   
## Data Sources & Reproducibility
1. **Fuente 1 — Siniestralidad vial en Cali (`siniestralidad_2016_2024.csv`)**: Dataset heredado de las entregas anteriores (371,003 registros, 2016–2024, codificación `latin1`, separador `;`). Colócalo en `data/raw/` para ejecución local; si no está presente, `extract.py` utiliza el respaldo reproducible (`cali_accidents_primary.csv`).
2. **Fuente 2 — Homicidios por condición de víctima (`homicidios_en_accidentes_de_transito_segun_condicion_de_victima_en_el_municipio_de_santiago_de_2.csv`)**: Dataset heredado de entregas previas (serie anual agregada 2004–2021). Como no incluye granularidad espacial por comuna/barrio ni índice de densidad vehicular, se mantiene un respaldo reproducible para el cruce espacial del pipeline.
3. **Fuente 3 — API Pública Open-Meteo**: Consulta perfiles horarios de temperatura, precipitación y viento para Cali (`3.4516, -76.5320`), con respaldo reproducible si el entorno no tiene acceso a internet.
