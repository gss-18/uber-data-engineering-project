import pandas as pd
import streamlit as st
import folium
from dotenv import load_dotenv
from folium.plugins import HeatMap
from streamlit_folium import st_folium

load_dotenv(override=True)

from components.ai_chat import render_ai_operations
from components.enhanced_charts import enhanced_bar_chart, enhanced_pie_chart, revenue_region_chart
from components.kpi_card import render_kpi_cards_html
from components.ride_table import render_ride_table
from data import generate_uber_ride_confirmation
from db import (
    get_kpis,
    get_live_feed,
    get_pickup_locations,
    get_revenue_by_region,
    get_rides_by_city,
    get_rides_by_payment,
    get_rides_by_vehicle_type,
    get_surge_distribution,
    get_top_drivers,
)
from design_tokens import CHART_COLORS, COLORS
from connection import send_to_event_hub


@st.cache_data(ttl=60)
def load_data():
    return {
        "kpis": get_kpis(),
        "by_city": get_rides_by_city(),
        "by_vehicle": get_rides_by_vehicle_type(),
        "by_payment": get_rides_by_payment(),
        "surge": get_surge_distribution(),
        "drivers": get_top_drivers(10),
        "live_feed": get_live_feed(15),
        "by_region": get_revenue_by_region(),
    }


def section(label: str, sublabel: str = "") -> None:
    cmt = f'<span class="s-cmt">// {sublabel}</span>' if sublabel else ""
    st.markdown(
        f'<div class="shd"><span class="s-eye">{label}</span>{cmt}</div>',
        unsafe_allow_html=True,
    )


@st.fragment
def action_bar() -> None:
    st.markdown('<div class="app-section" style="padding-top:1rem;">', unsafe_allow_html=True)
    col_book, col_refresh, col_num, col_status, _ = st.columns([1.2, 1, 0.8, 2.2, 3])

    with col_num:
        num_rides = st.number_input("rides", min_value=1, max_value=100, value=1, label_visibility="collapsed")

    with col_book:
        book_btn = st.button("Book Ride(s)", type="primary", use_container_width=True)

    with col_refresh:
        if st.button("Refresh", type="secondary", use_container_width=True):
            st.cache_data.clear()
            st.session_state.ai_insights = None
            st.rerun()

    with col_status:
        if book_btn:
            load_dotenv(override=True)
            eventhub_live = st.session_state.get("eventhub_live", False)
            if not eventhub_live:
                st.error("EventHub offline. Start it in Pipeline Control.")
            else:
                success = 0
                progress = st.progress(0)
                for i in range(num_rides):
                    ride = generate_uber_ride_confirmation()
                    if send_to_event_hub(ride):
                        success += 1
                    progress.progress((i + 1) / num_rides)
                progress.empty()
                st.success(f"{success}/{num_rides} rides sent to EventHub")

    st.markdown("</div>", unsafe_allow_html=True)


def render_kpis(k: dict) -> None:
    section("Performance metrics", "real-time aggregates from gold layer")
    render_kpi_cards_html(k)


def render_demand(data: dict) -> None:
    section("Demand analysis", "rides by city and vehicle type")
    st.markdown('<div class="app-section">', unsafe_allow_html=True)
    col1, col2 = st.columns([3, 2])

    with col1:
        df_city = pd.DataFrame(data["by_city"])
        if not df_city.empty:
            st.plotly_chart(
                enhanced_bar_chart(df_city, "city", "total_rides", "top 10 cities by rides"),
                use_container_width=True,
                config={"displayModeBar": False},
            )
        else:
            st.info("No city demand data available.")

    with col2:
        df_vehicle = pd.DataFrame(data["by_vehicle"])
        if not df_vehicle.empty:
            st.plotly_chart(
                enhanced_pie_chart(df_vehicle, "vehicle_type", "total_rides", "vehicle type split"),
                use_container_width=True,
                config={"displayModeBar": False},
            )
        else:
            st.info("No vehicle data available.")

    st.markdown("</div>", unsafe_allow_html=True)


def render_revenue(data: dict) -> None:
    section("Revenue intelligence", "payment methods and surge distribution")
    st.markdown('<div class="app-section">', unsafe_allow_html=True)
    col1, col2 = st.columns(2)

    with col1:
        df_payment = pd.DataFrame(data["by_payment"])
        if not df_payment.empty:
            st.plotly_chart(
                enhanced_pie_chart(df_payment, "payment_method", "total_rides", "payment methods"),
                use_container_width=True,
                config={"displayModeBar": False},
            )
        else:
            st.info("No payment data available.")

    with col2:
        df_surge = pd.DataFrame(data["surge"])
        if not df_surge.empty:
            st.plotly_chart(
                enhanced_bar_chart(df_surge, "surge_bucket", "ride_count", "surge multiplier distribution", COLORS["accent_gold"]),
                use_container_width=True,
                config={"displayModeBar": False},
            )
        else:
            st.info("No surge data available.")

    st.markdown("</div>", unsafe_allow_html=True)


def render_regions(data: dict) -> None:
    section("Regional breakdown", "revenue and rides across regions")
    st.markdown('<div class="app-section">', unsafe_allow_html=True)
    df_region = pd.DataFrame(data["by_region"])
    if not df_region.empty:
        st.plotly_chart(revenue_region_chart(df_region), use_container_width=True, config={"displayModeBar": False})
    else:
        st.info("No regional data available.")
    st.markdown("</div>", unsafe_allow_html=True)


def render_live_operations(data: dict) -> None:
    section("Live operations", "real-time ride feed and pickup heatmap")
    st.markdown('<div class="app-section">', unsafe_allow_html=True)
    col_feed, col_map = st.columns([3, 2])

    with col_feed:
        feed = data["live_feed"]
        if feed:
            df_feed = pd.DataFrame(feed)
            df_feed = _format_feed(df_feed)
            render_ride_table(df_feed, height=380)
        else:
            st.info("No live ride data available.")

    with col_map:
        locs = get_pickup_locations(200)
        if locs:
            df_locs = pd.DataFrame(locs).dropna(subset=["pickup_latitude", "pickup_longitude"])
            df_locs = df_locs[
                df_locs["pickup_latitude"].between(-90, 90)
                & df_locs["pickup_longitude"].between(-180, 180)
            ]
            if not df_locs.empty:
                m = folium.Map(location=[39.5, -98.35], zoom_start=3, tiles="CartoDB dark_matter")
                HeatMap(df_locs[["pickup_latitude", "pickup_longitude"]].values.tolist(), radius=12, blur=10, min_opacity=0.4).add_to(m)
                st_folium(m, use_container_width=True, height=380, returned_objects=[], key="pickup_heatmap")
            else:
                st.info("No valid coordinate data available.")
        else:
            st.info("No pickup data. Trigger the pipeline to load rides.")

    st.markdown("</div>", unsafe_allow_html=True)


def render_drivers(data: dict) -> None:
    section("Driver leaderboard", "ranked by total rides completed")
    st.markdown('<div class="app-section">', unsafe_allow_html=True)
    df_drivers = pd.DataFrame(data["drivers"])
    if not df_drivers.empty:
        df_drivers.columns = ["Driver", "Rating", "Rides", "Revenue ($)", "Avg Tip ($)"]
        df_drivers["Rank"] = [f"#{i + 1}" for i in range(len(df_drivers))]
        render_ride_table(df_drivers[["Rank", "Driver", "Rating", "Rides", "Revenue ($)", "Avg Tip ($)"]], height=360)
    else:
        st.info("No driver leaderboard data available.")
    st.markdown("</div>", unsafe_allow_html=True)


def _format_feed(df_feed: pd.DataFrame) -> pd.DataFrame:
    if "booking_timestamp" in df_feed.columns:
        df_feed["booking_timestamp"] = pd.to_datetime(df_feed["booking_timestamp"]).dt.strftime("%m/%d %H:%M")
    for col in ("rating", "driver_rating"):
        if col in df_feed.columns:
            df_feed[col] = df_feed[col].apply(lambda x: f"{float(x):.2f}" if pd.notna(x) and x is not None else "-")
    if "total_fare" in df_feed.columns:
        df_feed["total_fare"] = df_feed["total_fare"].apply(lambda x: f"${float(x):.2f}" if pd.notna(x) else "-")
    if "surge_multiplier" in df_feed.columns:
        df_feed["surge_multiplier"] = df_feed["surge_multiplier"].apply(lambda x: f"{float(x):.2f}x" if pd.notna(x) else "-")
    if "distance_miles" in df_feed.columns:
        df_feed["distance_miles"] = df_feed["distance_miles"].apply(lambda x: f"{float(x):.1f}mi" if pd.notna(x) else "-")
    if "duration_minutes" in df_feed.columns:
        df_feed["duration_minutes"] = df_feed["duration_minutes"].apply(lambda x: f"{int(x)}min" if pd.notna(x) else "-")

    return df_feed.drop(columns=["ride_id"], errors="ignore").rename(
        columns={
            "booking_timestamp": "Time",
            "passenger_name": "Passenger",
            "driver_name": "Driver",
            "driver_rating": "Driver Rating",
            "vehicle_type": "Vehicle",
            "pickup_city": "City",
            "distance_miles": "Dist",
            "duration_minutes": "Min",
            "surge_multiplier": "Surge",
            "total_fare": "Fare",
            "rating": "Rating",
        }
    )


action_bar()

with st.spinner(""):
    data = load_data()

st.markdown('<div class="app-section">', unsafe_allow_html=True)
render_ai_operations(data)
st.markdown("</div>", unsafe_allow_html=True)

render_kpis(data["kpis"])
render_demand(data)
render_revenue(data)
render_regions(data)
render_live_operations(data)
render_drivers(data)
