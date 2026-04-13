#!/usr/bin/env python3
"""Streamlit UI for NEM daily prices + BOM weather features.

Prefer the hub-integrated Flask UI (port 5020): start from index.html
"NEM prices & BOM weather", or: cd nem_weather_price_ml && python3 app.py

Optional Streamlit (repo root):
  streamlit run nem_weather_price_ml/dashboard.py
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd
import streamlit as st

from paths import data_processed

st.set_page_config(page_title="NEM prices & BOM weather", layout="wide")


@st.cache_data
def load_features(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values(["region", "date"])


def main() -> None:
    st.title("NEM daily prices and BOM weather")
    st.caption(
        "Loads `data/processed/features_daily.parquet`. "
        "Run `python nem_weather_price_ml/scripts/run_pipeline.py` first."
    )

    default_path = data_processed() / "features_daily.parquet"
    with st.sidebar:
        custom = st.text_input(
            "Parquet path (optional)",
            value="",
            help="Leave blank for default repo `data/processed/features_daily.parquet`.",
        )
        path = Path(custom).expanduser() if custom.strip() else default_path
        st.code(str(path), language="text")

    df = load_features(path)
    if df.empty:
        st.error(f"No data at `{path}`. Run the pipeline to build features.")
        return

    regions = sorted(df["region"].dropna().unique())
    r = st.sidebar.selectbox("Region", regions, index=0)
    d = df[df["region"] == r].copy()

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Rows", len(d))
    with c2:
        st.metric("From", str(d["date"].min().date()))
    with c3:
        st.metric("To", str(d["date"].max().date()))
    with c4:
        miss = int(d["weather_missing"].sum()) if "weather_missing" in d.columns else 0
        st.metric("Missing BOM days", miss)

    tab1, tab2, tab3 = st.tabs(["Price & temperature", "Scatter", "Raw peek"])

    with tab1:
        st.subheader(f"{r} — average RRP and daily max temperature")
        cL, cR = st.columns(2)
        with cL:
            st.caption("RRP average ($/MWh)")
            st.line_chart(d.set_index("date")["rrp_avg"])
        with cR:
            st.caption("BOM daily max temperature (°C)")
            st.line_chart(d.set_index("date")["tmax_c"])

    with tab2:
        st.subheader("Tmax vs average RRP (same day)")
        clean = d.dropna(subset=["tmax_c", "rrp_avg"])
        st.scatter_chart(clean, x="tmax_c", y="rrp_avg", height=420)

    with tab3:
        st.dataframe(d.tail(30), use_container_width=True)


if __name__ == "__main__":
    main()
