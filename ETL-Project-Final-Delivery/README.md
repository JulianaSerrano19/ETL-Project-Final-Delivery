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
   
## Data Sources, Schema & ML Target Definition

1. **Source Continuity (Deliverables 1, 2 & Final)**:
   - **Source 1 (`siniestralidad_2016_2024.csv`)**: Historical road traffic accident dataset for Cali (371,003 records, 2016–2024, `latin1` encoding, `;` delimiter). Read from `data/raw/` and preserves the full set of standardized records in `fact_siniestros`.
   - **Source 2 (`homicidios_en_accidentes_de_transito_segun_condicion_de_victima_en_el_municipio_de_santiago_de_2.csv`)**: Annual historical series (2004–2021, 5,254 fatalities by victim condition). Because it lacks spatial granularity by comuna/neighborhood and traffic density metrics, it populates the historical dimension `dim_victimas`, while a reproducible fallback supplies spatial attributes (`zona`, `via_tipo`, `traffic_density_index`) for pipeline integration.
   - **Source 3 (Open-Meteo API)**: Hourly profiles of temperature, precipitation, and wind speed for Cali (`3.4516, -76.5320`), with an offline reproducible fallback.

2. **Cleaning, Quality & Dimensional Model**:
   - In `src/etl/transform.py`, duplicate columns are removed, corrupted characters (`latin1`) are fixed, missing-value markers (`.`, `SD`, nulls) are standardized, and dates and numeric fields are properly typed.
   - In `src/quality/validator.py`, `incident_id` uniqueness, temporal integrity, clean domains, and numeric ranges are validated before loading the star schema (`fact_siniestros`, `dim_tiempo`, `dim_victimas`, `dim_clima_movilidad`).

3. **ML Problem Definition (`es_grave`) Without Data Leakage**:
   - **Target variable (`es_grave`)**: Binary (`1` = confirmed crash with injured or deceased victims [50,860 cases / 13.71%]; `0` = crash without injuries or fatalities [320,143 cases / 86.29%]).
   - **Data leakage prevention**: Outcome columns (`tipo_confirmado`, `tipo_confirmado_limpio`, victim counts) are excluded from predictor features (`X`) during both model training and Kafka event streaming.
  
## System Architecture

![Cali Mobility Data Warehouse & Real-Time Risk Streaming Architecture](docs/architecture_diagram.png)

The architecture integrates two complementary workflows over a unified data backbone:
- **Analytical Batch Workflow:** Ingests the three data sources, validates schema and quality rules in Apache Airflow, loads the SQLite Star Schema (`fact_siniestros` + dimensional tables), and powers the static Python analytical report and EDA notebooks.
- **Operational Real-Time Workflow:** Trains and serializes the crash severity model (`accident_severity_pipeline.pkl`), publishes leak-free incident events to Apache Kafka (`cali_traffic_incidents_stream`), scores incoming events in real time via the Kafka consumer, persists alerts in `fact_predicciones_streaming`, and updates the live Streamlit dashboard.
