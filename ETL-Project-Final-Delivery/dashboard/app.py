import json
import streamlit as st
import pandas as pd
import joblib
from kafka import KafkaConsumer

from ml.train_model import CATEGORICAL_FEATURES, NUMERIC_FEATURES
from src.utils.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_ACCIDENTS, MODEL_PATH

st.set_page_config(page_title="Cali Real-Time Crash Severity Stream", layout="wide")
st.title("Cali Traffic Severity Real-Time Predictions (Kafka Stream)")

@st.cache_resource
def load_model():
    return joblib.load(MODEL_PATH)

@st.cache_resource
def get_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC_ACCIDENTS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        consumer_timeout_ms=500,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

if "stream_records" not in st.session_state:
    st.session_state.stream_records = []

model = load_model()
consumer = get_consumer()

col_btn1, col_btn2 = st.columns([1, 5])
with col_btn1:
    poll_clicked = st.button("Poll Kafka Messages")
with col_btn2:
    auto_refresh = st.checkbox("Auto-poll stream", value=True)

if poll_clicked or auto_refresh:
    new_rows = []
    for msg in consumer:
        event = msg.value
        df_feat = pd.DataFrame([event])
        for c in NUMERIC_FEATURES + CATEGORICAL_FEATURES:
            if c not in df_feat.columns:
                df_feat[c] = None
        pred = int(model.predict(df_feat[NUMERIC_FEATURES + CATEGORICAL_FEATURES])[0])
        prob = float(model.predict_proba(df_feat[NUMERIC_FEATURES + CATEGORICAL_FEATURES])[0, 1])
        alert = "HIGH" if prob >= 0.65 else ("MEDIUM" if prob >= 0.40 else "LOW")
        new_rows.append({
            "event_id": event.get("event_id", ""),
            "incident_id": event.get("incident_id", ""),
            "comuna": event.get("comuna", "Unassigned"),
            "clase_accidente": event.get("clase_accidente", "Unreported"),
            "vehiculos_implicados": event.get("vehiculos_implicados", "Unreported"),
            "hora_hecho": event.get("hora_hecho", 0),
            "severe_probability": round(prob, 4),
            "predicted_is_severe": pred,
            "alert_level": alert,
        })

    if new_rows:
        st.session_state.stream_records.extend(new_rows)
        st.session_state.stream_records = st.session_state.stream_records[-200:]

df_live = pd.DataFrame(st.session_state.stream_records)

m1, m2, m3 = st.columns(3)
m1.metric("Consumed Events", len(df_live))
m2.metric("Predicted Severe (`es_grave = 1`)", int(df_live["predicted_is_severe"].sum()) if not df_live.empty else 0)
m3.metric(
    "Avg Severe Probability",
    f"{df_live['severe_probability'].mean():.2%}" if not df_live.empty else "0.00%",
)

if not df_live.empty:
    st.subheader("Live Stream Predictions")
    st.dataframe(df_live.iloc[::-1], use_container_width=True)
    st.subheader("Severe Probability Trend Across Consumed Events")
    st.line_chart(df_live["severe_probability"].reset_index(drop=True))
else:
    st.info("Waiting for incoming events from Kafka topic... Run `python -m kafka.producer` in another terminal.")