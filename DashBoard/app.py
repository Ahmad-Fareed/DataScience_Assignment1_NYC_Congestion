import streamlit as st
import pandas as pd
import os

st.set_page_config(layout="wide")

st.title("NYC Congestion Pricing Audit Dashboard")

# ---------- Fix paths ----------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

monthly_kpis = pd.read_parquet(os.path.join(BASE_DIR, "monthly_kpis.parquet"))

monthly_kpis["month"] = pd.to_datetime(monthly_kpis["month"])
monthly_kpis = monthly_kpis.sort_values("month")


zone_counts = pd.read_parquet(os.path.join(BASE_DIR, "dashboard_zone_counts.parquet"))
leakage = pd.read_parquet(os.path.join(BASE_DIR, "dashboard_leakage.parquet"))
velocity = pd.read_parquet(os.path.join(BASE_DIR, "velocity_heatmap.parquet"))


# ---------- KPI Overview ----------
st.header("Monthly KPIs")

col1, col2 = st.columns(2)

with col1:
    st.line_chart(
        monthly_kpis.set_index("month")["total_trips"]
    )

with col2:
    st.line_chart(
        monthly_kpis.set_index("month")["total_revenue"]
    )

# ---------- Top Pickup Zones ----------
st.header("Top Pickup Zones")

top_zones = zone_counts.head(10)
st.bar_chart(top_zones.set_index("pickup_loc")["trip_count"])

# ---------- Leakage Summary ----------
st.header("Congestion Leakage")

st.dataframe(leakage)
st.header("Overall Summary")

col1, col2, col3 = st.columns(3)

col1.metric("Total Trips",
            f"{monthly_kpis.total_trips.sum():,.0f}")

col2.metric("Total Revenue",
            f"${monthly_kpis.total_revenue.sum():,.0f}")

col3.metric("Avg Monthly Trips",
            f"{monthly_kpis.total_trips.mean():,.0f}")

# ---------- Velocity Heatmap Data ----------
st.header("Average Speed by Hour")

pivot = velocity.pivot(
    index="weekday",
    columns="hour",
    values="avg_speed"
)

st.dataframe(pivot)

st.success("Dashboard loaded successfully.")
