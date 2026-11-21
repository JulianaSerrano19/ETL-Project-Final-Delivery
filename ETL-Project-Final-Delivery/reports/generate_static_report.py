import logging
import sqlite3
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from src.utils.config import DB_PATH, REPORT_OUTPUT_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def generate_analytical_report(db_path: Path = DB_PATH, output_dir: Path = REPORT_OUTPUT_DIR) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)

    try:
        df_annual = pd.read_sql("SELECT * FROM vw_resumen_anual_gravedad;", conn)
        df_risk = pd.read_sql("SELECT * FROM vw_riesgo_por_vehiculo_clima LIMIT 10;", conn)
        try:
            df_victims = pd.read_sql(
                "SELECT * FROM dim_victimas_historico ORDER BY total_2004_2021 DESC;", conn
            )
        except Exception:
            df_victims = pd.DataFrame()
    finally:
        conn.close()

    # Chart 1: Annual severe crash rate
    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.bar(df_annual["anio"].astype(str), df_annual["tasa_gravedad_pct"], color="#6A4C93")
    ax.set_title("Annual Severe Crash Rate (%) in Cali")
    ax.set_xlabel("Year")
    ax.set_ylabel("Severe Crash Rate (%)")
    plt.tight_layout()
    chart1_path = output_dir / "annual_severity_rate.png"
    plt.savefig(chart1_path, dpi=150)
    plt.close(fig)

    # Chart 2: Top vehicle & accident class severity risk
    fig, ax = plt.subplots(figsize=(10, 5))
    labels = df_risk["vehiculos_implicados"] + " | " + df_risk["clase_accidente"]
    ax.barh(labels[::-1], df_risk["tasa_gravedad_pct"][::-1], color="#8E6CC3")
    ax.set_title("Top Severity Rates by Vehicle & Accident Class")
    ax.set_xlabel("Severe Crash Rate (%)")
    plt.tight_layout()
    chart2_path = output_dir / "top_risk_combinations.png"
    plt.savefig(chart2_path, dpi=150)
    plt.close(fig)

    total_events = int(df_annual["total_siniestros"].sum()) if not df_annual.empty else 0
    total_severe = int(df_annual["total_graves"].sum()) if not df_annual.empty else 0
    overall_rate = round(100.0 * total_severe / max(total_events, 1), 2)

    top_victim = (
        f"{df_victims.iloc[0]['condicion_victima']} ({df_victims.iloc[0]['total_2004_2021']} fatalities, "
        f"{df_victims.iloc[0]['participacion_pct']}%)"
        if not df_victims.empty
        else "Historical victim dimension not loaded"
    )

    report_md = output_dir / "mobility_analytical_report.md"
    content = f"""# Cali Mobility Analytical Report

## Executive Summary & Key Insights
- **Total Analyzed Incidents**: {total_events:,} records across the star schema.
- **Confirmed Severe Incidents (`es_grave = 1`)**: {total_severe:,} ({overall_rate}% overall severe rate).
- **Historical Victim Vulnerability (2004-2021)**: Highest fatality burden corresponds to **{top_victim}**.
- **Operational Takeaway**: Severe injury/fatality risk concentrates in vulnerable road users and specific vehicle/collision profiles, supporting targeted patrol allocation and weather-aware alerts.

## Generated Charts
- Annual Severity Rate: `{chart1_path.name}`
- Top Risk Combinations: `{chart2_path.name}`
"""
    report_md.write_text(content, encoding="utf-8")
    logger.info(f"Static analytical report generated at {report_md}")
    return report_md


if __name__ == "__main__":
    generate_analytical_report()