import streamlit as st
import pandas as pd
import os

st.set_page_config(layout="wide")

st.title("NYC Congestion Pricing Audit Dashboard")

# ---------- Base Path ----------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ---------- Load Data ----------
monthly_kpis = pd.read_parquet(
    os.path.join(BASE_DIR, "monthly_kpis.parquet")
)

monthly_kpis["month"] = pd.to_datetime(monthly_kpis["month"])
monthly_kpis = monthly_kpis.sort_values("month")

zone_counts = pd.read_parquet(
    os.path.join(BASE_DIR, "dashboard_zone_counts.parquet")
)

leakage = pd.read_parquet(
    os.path.join(BASE_DIR, "dashboard_leakage.parquet")
)

velocity = pd.read_parquet(
    os.path.join(BASE_DIR, "velocity_heatmap.parquet")
)

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

# ---------- Tabs ----------
tab1, tab2, tab3, tab4 = st.tabs([
    "Border Effect Map",
    "Traffic Flow",
    "Economics",
    "Weather Impact"
])

# ---------- TAB 1: Border Effect ----------
with tab1:
    st.header("Border Effect Analysis")

    border_effect = pd.read_parquet(
        os.path.join(BASE_DIR, "border_effect.parquet")
    )

    if len(border_effect) == 0:
        st.warning("No border effect data available.")
    else:
        st.dataframe(border_effect)

    st.info("Trips ending near congestion boundary zones.")

# ---------- TAB 2: Traffic Flow ----------
with tab2:
    st.header("Velocity Heatmap")

    pivot = velocity.pivot(
        index="weekday",
        columns="hour",
        values="avg_speed"
    )

    st.dataframe(pivot)

# ---------- TAB 3: Economics ----------
with tab3:
    st.header("Tip vs Surcharge Analysis")

    crowding = pd.read_parquet(
        os.path.join(BASE_DIR, "crowding_out.parquet")
    )

    crowding["month"] = pd.to_datetime(crowding["month"])

    st.line_chart(
        crowding.set_index("month")[
            ["avg_surcharge", "avg_tip_ratio"]
        ]
    )

# ---------- TAB 4: Weather ----------
with tab4:
    st.header("Rain Impact on Taxi Demand")

    rain = pd.read_parquet(
        os.path.join(BASE_DIR, "rain_tax.parquet")
    )

    st.dataframe(rain)

    # rainy: 0 = no rain, 1 = rain
    st.scatter_chart(
        rain.set_index("rainy")["avg_trips"]
    )

st.success("Dashboard loaded successfully.")
