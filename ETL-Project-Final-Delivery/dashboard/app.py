import time
import sqlite3
import pandas as pd
import streamlit as st
import plotly.express as px
from src.utils.config import DB_PATH

st.set_page_config(
    page_title="Cali Mobility Real-Time Risk Monitor",
    page_icon="🚦",
    layout="wide"
)

st.title("🚦 Real-Time Traffic Incident Severity Prediction Dashboard")
st.markdown(
    "Live operational monitoring driven by **Apache Kafka** + serialized **RandomForest ML pipeline**."
)

refresh_sec = st.sidebar.slider("Auto-refresh interval (seconds)", min_value=1, max_value=10, value=2)
st.sidebar.markdown("### Architecture Flow")
st.sidebar.info(
    "1. Kafka Producer emits incident features\n"
    "2. Kafka Consumer runs `.pkl` inference\n"
    "3. Predictions persist to operational table\n"
    "4. Dashboard renders live updates"
)


def load_live_predictions(limit: int = 200) -> pd.DataFrame:
    if not DB_PATH.exists():
        return pd.DataFrame()
    limit = int(limit)
    query = f"""
    SELECT
        event_id,
        incident_id,
        event_timestamp,
        comuna,
        clase_vehiculo,
        hour_of_day,
        temperature_c,
        precipitation_mm,
        traffic_density_index,
        predicted_severe_risk,
        severe_probability,
        risk_level,
        processed_at
    FROM streaming_incident_predictions
    ORDER BY processed_at DESC
    LIMIT {limit}
    """
    try:
        with sqlite3.connect(DB_PATH) as conn:
            return pd.read_sql_query(query, conn)
    except Exception:
        return pd.DataFrame()


df = load_live_predictions()

if df.empty:
    st.warning("Waiting for incoming Kafka consumer predictions... Start `kafka/consumer.py` and `kafka/producer.py`.")
else:
    df["precipitation_size"] = df["precipitation_mm"].fillna(0).clip(lower=0) + 0.1

    total_processed = len(df)
    high_risk_count = int((df["risk_level"] == "HIGH").sum())
    avg_prob = df["severe_probability"].mean() * 100
    latest_event = df.iloc[0]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Streamed Events Processed", f"{total_processed}")
    col2.metric("High-Risk Alerts (Prob >= 70%)", f"{high_risk_count}")
    col3.metric("Mean Severe Injury Probability", f"{avg_prob:.1f}%")
    col4.metric("Latest Incident Risk", f"{latest_event['risk_level']} ({latest_event['severe_probability']*100:.1f}%)")

    c1, c2 = st.columns(2)
    with c1:
        risk_counts = df["risk_level"].value_counts().reset_index()
        risk_counts.columns = ["Risk Level", "Count"]
        fig_risk = px.bar(
            risk_counts,
            x="Risk Level",
            y="Count",
            color="Risk Level",
            color_discrete_map={"HIGH": "#d62728", "MEDIUM": "#ff7f0e", "LOW": "#2ca02c"},
            title="Live Predictions by Risk Category"
        )
        st.plotly_chart(fig_risk, use_container_width=True)

    with c2:
        fig_scatter = px.scatter(
            df,
            x="traffic_density_index",
            y="severe_probability",
            color="clase_vehiculo",
            size="precipitation_size",
            hover_data=["incident_id", "comuna", "hour_of_day", "precipitation_mm"],
            title="Predicted Severity Probability vs. Traffic Density & Rain"
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

    st.subheader("Live Stream Inference Feed (Most Recent Events)")
    st.dataframe(
        df[[
            "processed_at", "incident_id", "comuna", "clase_vehiculo",
            "hour_of_day", "precipitation_mm", "traffic_density_index",
            "severe_probability", "risk_level"
        ]].head(20),
        use_container_width=True
    )

time.sleep(refresh_sec)
st.rerun()