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
st.header("KPIs — Whole Year 2025")

monthly_kpis["year"] = monthly_kpis["month"].dt.year
kpi_2025 = monthly_kpis[monthly_kpis["year"] == 2025]

total_trips = kpi_2025["total_trips"].sum()
total_revenue = kpi_2025["total_revenue"].sum()
congestion_revenue = kpi_2025["congestion_revenue"].sum()
avg_distance = kpi_2025["avg_distance"].mean()

c1, c2, c3, c4 = st.columns(4)

c1.metric("Total Trips", f"{total_trips:,.0f}")
c2.metric("Total Revenue ($)", f"{total_revenue:,.0f}")
c3.metric("Congestion Revenue ($)", f"{congestion_revenue:,.0f}")
c4.metric("Avg Distance (mi)", f"{avg_distance:.2f}")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Trips Trend")
    st.line_chart(
        monthly_kpis.set_index("month")["total_trips"]
    )

with col2:
    st.subheader("Revenue Trend")
    st.line_chart(
        monthly_kpis.set_index("month")[
            ["total_revenue", "congestion_revenue"]
        ]
    )
st.subheader("Before vs After Congestion Pricing")

# Define policy years
before = monthly_kpis[monthly_kpis["year"] == 2024]
after = monthly_kpis[monthly_kpis["year"] == 2025]

if len(before) > 0 and len(after) > 0:

    before_trips = before["total_trips"].sum()
    after_trips = after["total_trips"].sum()

    before_rev = before["total_revenue"].sum()
    after_rev = after["total_revenue"].sum()

    before_speed = before["avg_duration_minutes"].mean()
    after_speed = after["avg_duration_minutes"].mean()

    c1, c2, c3 = st.columns(3)

    c1.metric(
        "Trips Change",
        f"{after_trips:,.0f}",
        f"{(after_trips-before_trips)/before_trips*100:.1f}%"
    )

    c2.metric(
        "Revenue Change ($)",
        f"{after_rev:,.0f}",
        f"{(after_rev-before_rev)/before_rev*100:.1f}%"
    )

    c3.metric(
        "Avg Duration Change (min)",
        f"{after_speed:.2f}",
        f"{after_speed-before_speed:.2f}"
    )

# ---------- Tabs ----------
tab1, tab2, tab3, tab4 = st.tabs([
    "Border Effect Map",
    "Traffic Flow",
    "Economics",
    "Weather Impact"
])

# ---------- TAB 1 ----------
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

# ---------- TAB 2 ----------
with tab2:
    st.header("Velocity Heatmap")

    pivot = velocity.pivot(
        index="weekday",
        columns="hour",
        values="avg_speed"
    )

    st.dataframe(pivot)

# ---------- TAB 3 ----------
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

    # Revenue zones
    st.subheader("Top Revenue Pickup Zones")

    top_rev = zone_counts.sort_values(
        "revenue", ascending=False
    ).head(10)

    st.bar_chart(
        top_rev.set_index("pickup_loc")["revenue"]
    )

    # Leakage revenue
    st.subheader("Revenue Lost Due to Leakage")

    leakage["month"] = pd.to_datetime(leakage["month"])

    st.line_chart(
        leakage.set_index("month")["leakage_revenue"]
    )

# ---------- TAB 4 ----------
with tab4:
    st.header("Rain Impact on Taxi Demand")

    rain = pd.read_parquet(
        os.path.join(BASE_DIR, "rain_tax.parquet")
    )

    st.dataframe(rain)

    st.subheader("Rain vs Demand")

    # Pipeline outputs avg_trips
    st.scatter_chart(
    rain,
    x="rainy",
    y="trip_count"
)


st.success("Dashboard loaded successfully.")
