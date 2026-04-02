import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import os
import sys
import folium
from folium.plugins import HeatMap
from streamlit_folium import st_folium
from dotenv import load_dotenv
load_dotenv(override=True)

import importlib
import db as _db_module
importlib.reload(_db_module)
from db import (
    get_kpis, get_rides_by_city, get_rides_by_vehicle_type,
    get_rides_by_payment, get_surge_distribution, get_top_drivers,
    get_live_feed, get_pickup_locations, get_revenue_by_region
)
from connection import send_to_event_hub
from data import generate_uber_ride_confirmation

PLOT_LAYOUT = dict(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Space Mono, monospace", color="rgba(255,255,255,0.5)", size=10),
    margin=dict(l=0, r=0, t=24, b=0),
    xaxis=dict(gridcolor="rgba(255,255,255,0.05)", linecolor="rgba(255,255,255,0.1)", tickfont=dict(size=9)),
    yaxis=dict(gridcolor="rgba(255,255,255,0.05)", linecolor="rgba(255,255,255,0.1)", tickfont=dict(size=9)),
    hoverlabel=dict(bgcolor="#0d1117", font_family="Space Mono, monospace", font_size=11),
)

PIE_LAYOUT = dict(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Space Mono, monospace", color="rgba(255,255,255,0.5)", size=10),
    margin=dict(l=0, r=0, t=24, b=0),
    hoverlabel=dict(bgcolor="#0d1117", font_family="Space Mono, monospace", font_size=11),
    showlegend=True,
    legend=dict(orientation="v", x=1, y=0.5, font=dict(size=9, family="Space Mono"), bgcolor="rgba(0,0,0,0)"),
)


@st.cache_data(ttl=60)
def load_data():
    return {
        "kpis":       get_kpis(),
        "by_city":    get_rides_by_city(),
        "by_vehicle": get_rides_by_vehicle_type(),
        "by_payment": get_rides_by_payment(),
        "surge":      get_surge_distribution(),
        "drivers":    get_top_drivers(10),
        "live_feed":  get_live_feed(15),
        "by_region":  get_revenue_by_region(),
    }


def section(label, sublabel=""):
    st.markdown(f"""
    <div style="display:flex;align-items:center;gap:1rem;padding:1.5rem 2rem 0.75rem;border-top:1px solid rgba(255,255,255,0.04);">
        <div style="font-family:'Space Mono',monospace;font-size:0.6rem;color:#00ffc8;letter-spacing:0.2em;text-transform:uppercase;">{label}</div>
        {'<div style="font-family:Space Mono,monospace;font-size:0.55rem;color:rgba(255,255,255,0.2);letter-spacing:0.1em;">// ' + sublabel + '</div>' if sublabel else ''}
    </div>
    """, unsafe_allow_html=True)


# ── Top action bar ─────────────────────────────────────────────────
@st.fragment
def action_bar():
    st.markdown('<div style="padding: 1.5rem 2rem 0;">', unsafe_allow_html=True)
    col_book, col_refresh, col_num, col_status, _ = st.columns([1.2, 1, 0.8, 2, 3])

    with col_num:
        num_rides = st.number_input("rides", min_value=1, max_value=100, value=1, label_visibility="collapsed")

    with col_book:
        book_btn = st.button("⚡ Book Ride(s)", type="primary", use_container_width=True)

    with col_refresh:
        if st.button("↻ Refresh", type="secondary", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    with col_status:
        if book_btn:
            load_dotenv(override=True)
            eventhub_live = bool(os.getenv("CONNECTION_STRING"))
            if not eventhub_live:
                st.error("EventHub offline — start it in Pipeline Control tab")
            else:
                success = 0
                progress = st.progress(0)
                for i in range(num_rides):
                    ride = generate_uber_ride_confirmation()
                    if send_to_event_hub(ride):
                        success += 1
                    progress.progress((i + 1) / num_rides)
                progress.empty()
                st.success(f"✓ {success}/{num_rides} rides sent to EventHub")

    st.markdown('</div>', unsafe_allow_html=True)

action_bar()

# ── Load data ──────────────────────────────────────────────────────
with st.spinner(""):
    data = load_data()

k = data["kpis"]

# ── KPIs ───────────────────────────────────────────────────────────
section("Performance metrics", "real-time aggregates from gold layer")
st.markdown('<div style="padding: 0 2rem 1.5rem;">', unsafe_allow_html=True)

kpi_cols = st.columns(7)
kpis = [
    ("Total Rides",      f"{int(k['total_rides']):,}",              "rides"),
    ("Revenue",          f"${int(k['total_revenue']):,}",            "USD"),
    ("Avg Fare",         f"${k['avg_fare']}",                        "per ride"),
    ("Avg Surge",        f"{k['avg_surge']}×",                       "multiplier"),
    ("Avg Rating",       f"{k['avg_rating']}",                       "/ 5.0"),
    ("Avg Distance",     f"{k['avg_distance']}",                     "miles"),
    ("Cancellation",     f"{k.get('cancellation_rate', '—')}%",      "rate"),
]

for col, (label, value, unit) in zip(kpi_cols, kpis):
    with col:
        st.markdown(f"""
        <div style="border:1px solid rgba(0,255,200,0.12);background:rgba(0,255,200,0.03);padding:1.25rem 1rem;border-radius:2px;position:relative;overflow:hidden;">
            <div style="position:absolute;top:0;left:0;width:2px;height:100%;background:#00ffc8;opacity:0.6;"></div>
            <div style="font-family:'Space Mono',monospace;font-size:0.55rem;color:rgba(255,255,255,0.35);letter-spacing:0.15em;text-transform:uppercase;margin-bottom:0.5rem;">{label}</div>
            <div style="font-family:'Space Mono',monospace;font-size:1.5rem;font-weight:700;color:white;line-height:1;">{value}</div>
            <div style="font-family:'Space Mono',monospace;font-size:0.55rem;color:rgba(255,255,255,0.2);margin-top:0.3rem;">{unit}</div>
        </div>
        """, unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)

# ── Charts row 1 ───────────────────────────────────────────────────
section("Demand analysis", "rides by city and vehicle type")
st.markdown('<div style="padding: 0 2rem 1.5rem;">', unsafe_allow_html=True)

col1, col2 = st.columns([3, 2])

with col1:
    df_city = pd.DataFrame(data["by_city"])
    if not df_city.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df_city["city"],
            y=df_city["total_rides"],
            marker=dict(
                color=df_city["total_rides"],
                colorscale=[[0, "rgba(0,255,200,0.3)"], [1, "#00ffc8"]],
                line=dict(width=0)
            ),
            hovertemplate="<b>%{x}</b><br>Rides: %{y}<extra></extra>",
        ))
        fig.update_layout(
            **PLOT_LAYOUT,
            height=260,
            xaxis_title=None,
            yaxis_title=None,
            showlegend=False,
            title=dict(text="top 10 cities by rides", font=dict(size=10, family="Space Mono", color="rgba(255,255,255,0.3)"), x=0)
        )
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

with col2:
    df_v = pd.DataFrame(data["by_vehicle"])
    if not df_v.empty:
        fig = go.Figure(go.Pie(
            labels=df_v["vehicle_type"],
            values=df_v["total_rides"],
            hole=0.65,
            marker=dict(colors=["#00ffc8","#00b8ff","#ffb800","#ff4466","#a855f7"]),
            textinfo="percent",
            textfont=dict(family="Space Mono", size=9),
            hovertemplate="<b>%{label}</b><br>%{value} rides (%{percent})<extra></extra>",
        ))
        pie_layout_vehicle = {**PIE_LAYOUT, "legend": dict(orientation="v", x=1.02, y=0.5, font=dict(size=9, family="Space Mono"), bgcolor="rgba(0,0,0,0)")}
        fig.update_layout(
            **pie_layout_vehicle,
            height=260,
            title=dict(text="vehicle type split", font=dict(size=10, family="Space Mono", color="rgba(255,255,255,0.3)"), x=0),
            annotations=[dict(text="vehicle<br>type", x=0.38, y=0.5, font=dict(size=9, family="Space Mono", color="rgba(255,255,255,0.4)"), showarrow=False)]
        )
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

st.markdown('</div>', unsafe_allow_html=True)

# ── Charts row 2 ───────────────────────────────────────────────────
section("Revenue intelligence", "payment methods and surge pricing distribution")
st.markdown('<div style="padding: 0 2rem 1.5rem;">', unsafe_allow_html=True)

col3, col4 = st.columns(2)

with col3:
    df_p = pd.DataFrame(data["by_payment"])
    if not df_p.empty:
        fig = go.Figure(go.Pie(
            labels=df_p["payment_method"],
            values=df_p["total_rides"],
            hole=0.65,
            marker=dict(colors=["#ffb800","#ff4466","#00b8ff","#a855f7"]),
            textfont=dict(family="Space Mono", size=9),
            hovertemplate="<b>%{label}</b><br>%{value} rides (%{percent})<extra></extra>",
        ))
        fig.update_layout(
            **PIE_LAYOUT,
            height=240,
            title=dict(text="payment methods", font=dict(size=10, family="Space Mono", color="rgba(255,255,255,0.3)"), x=0),
            annotations=[dict(text="payment", x=0.38, y=0.5, font=dict(size=9, family="Space Mono", color="rgba(255,255,255,0.4)"), showarrow=False)]
        )
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

with col4:
    df_s = pd.DataFrame(data["surge"])
    if not df_s.empty:
        fig = go.Figure(go.Bar(
            x=df_s["surge_bucket"],
            y=df_s["ride_count"],
            marker=dict(
                color=df_s["surge_bucket"],
                colorscale=[[0, "rgba(0,255,200,0.5)"], [0.5, "rgba(255,184,0,0.7)"], [1, "rgba(255,68,102,0.85)"]],
                line=dict(width=0),
            ),
            hovertemplate="<b>%{x}×</b><br>Rides: %{y:,}<extra></extra>",
        ))
        fig.update_layout(
            **PLOT_LAYOUT,
            height=240,
            xaxis_title="surge multiplier",
            yaxis_title="rides",
            showlegend=False,
            title=dict(text="surge multiplier distribution", font=dict(size=10, family="Space Mono", color="rgba(255,255,255,0.3)"), x=0),
        )
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

st.markdown('</div>', unsafe_allow_html=True)

# ── Charts row 3 ───────────────────────────────────────────────────
section("Regional breakdown", "revenue and rides across 5 US regions")
st.markdown('<div style="padding: 0 2rem 1.5rem;">', unsafe_allow_html=True)

df_region = pd.DataFrame(data["by_region"])
if not df_region.empty:
    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Revenue ($)",
        x=df_region["region"],
        y=df_region["total_revenue"],
        marker=dict(color="#00ffc8", opacity=0.85, line=dict(width=0)),
        hovertemplate="<b>%{x}</b><br>Revenue: $%{y:,.0f}<extra></extra>",
        yaxis="y1",
    ))
    fig.add_trace(go.Scatter(
        name="Rides",
        x=df_region["region"],
        y=df_region["total_rides"],
        mode="markers+lines",
        marker=dict(color="#ffb800", size=8),
        line=dict(color="#ffb800", width=2, dash="dot"),
        hovertemplate="<b>%{x}</b><br>Rides: %{y:,}<extra></extra>",
        yaxis="y2",
    ))
    region_layout = {
        **PLOT_LAYOUT,
        "yaxis":  dict(title="revenue ($)", gridcolor="rgba(255,255,255,0.05)", tickfont=dict(size=9)),
        "legend": dict(orientation="h", x=0, y=1.1, font=dict(size=9, family="Space Mono"), bgcolor="rgba(0,0,0,0)"),
    }
    fig.update_layout(
        **region_layout,
        height=260,
        xaxis_title=None,
        yaxis2=dict(title="rides", overlaying="y", side="right", gridcolor="rgba(0,0,0,0)", tickfont=dict(size=9)),
        title=dict(text="revenue & rides by region", font=dict(size=10, family="Space Mono", color="rgba(255,255,255,0.3)"), x=0),
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

st.markdown('</div>', unsafe_allow_html=True)

# ── Live feed + map ────────────────────────────────────────────────
section("Live operations", "real-time ride feed and pickup heatmap")
st.markdown('<div style="padding: 0 2rem 1.5rem;">', unsafe_allow_html=True)

col_feed, col_map = st.columns([3, 2])

with col_feed:
    st.markdown("""
    <div style="font-family:'Space Mono',monospace;font-size:0.6rem;color:rgba(255,255,255,0.25);letter-spacing:0.15em;margin-bottom:0.75rem;">
        LAST 15 RIDES
    </div>
    """, unsafe_allow_html=True)

    feed = data["live_feed"]
    if feed:
        df_feed = pd.DataFrame(feed)

        if "booking_timestamp" in df_feed.columns:
            df_feed["booking_timestamp"] = pd.to_datetime(df_feed["booking_timestamp"]).dt.strftime("%m/%d %H:%M")
        if "rating" in df_feed.columns:
            df_feed["rating"] = df_feed["rating"].apply(
                lambda x: f"{float(x):.1f}" if pd.notna(x) and x is not None else "—"
            )
        if "driver_rating" in df_feed.columns:
            df_feed["driver_rating"] = df_feed["driver_rating"].apply(
                lambda x: f"{float(x):.2f}" if pd.notna(x) and x is not None else "—"
            )
        if "total_fare" in df_feed.columns:
            df_feed["total_fare"] = df_feed["total_fare"].apply(
                lambda x: f"${float(x):.2f}" if pd.notna(x) else "—"
            )
        if "surge_multiplier" in df_feed.columns:
            df_feed["surge_multiplier"] = df_feed["surge_multiplier"].apply(
                lambda x: f"{float(x):.2f}×" if pd.notna(x) else "—"
            )
        if "distance_miles" in df_feed.columns:
            df_feed["distance_miles"] = df_feed["distance_miles"].apply(
                lambda x: f"{float(x):.1f}mi" if pd.notna(x) else "—"
            )
        if "duration_minutes" in df_feed.columns:
            df_feed["duration_minutes"] = df_feed["duration_minutes"].apply(
                lambda x: f"{int(x)}min" if pd.notna(x) else "—"
            )

        df_feed = df_feed.drop(columns=["ride_id"], errors="ignore")
        df_feed = df_feed.rename(columns={
            "booking_timestamp": "Time",
            "passenger_name":    "Passenger",
            "driver_name":       "Driver",
            "driver_rating":     "Drv ★",
            "vehicle_type":      "Vehicle",
            "pickup_city":       "City",
            "distance_miles":    "Dist",
            "duration_minutes":  "Min",
            "surge_multiplier":  "Surge",
            "total_fare":        "Fare",
            "rating":            "★",
        })
        st.dataframe(df_feed, use_container_width=True, hide_index=True, height=380)

with col_map:
    st.markdown("""
    <div style="font-family:'Space Mono',monospace;font-size:0.6rem;color:rgba(255,255,255,0.25);letter-spacing:0.15em;margin-bottom:0.75rem;">
        PICKUP LOCATIONS — last 200 rides
    </div>
    """, unsafe_allow_html=True)

    @st.cache_data(ttl=120)
    def load_map_data():
        return get_pickup_locations(200)

    locs = load_map_data()
    if locs:
        df_locs = pd.DataFrame(locs).dropna(subset=["pickup_latitude","pickup_longitude"])
        df_locs = df_locs[
            df_locs["pickup_latitude"].between(-90, 90) &
            df_locs["pickup_longitude"].between(-180, 180)
        ]
        m = folium.Map(location=[39.5, -98.35], zoom_start=3, tiles="CartoDB dark_matter")
        heat_data = df_locs[["pickup_latitude", "pickup_longitude"]].values.tolist()
        HeatMap(heat_data, radius=12, blur=10, min_opacity=0.4).add_to(m)
        st_folium(m, use_container_width=True, height=380)

st.markdown('</div>', unsafe_allow_html=True)

# ── Top drivers ────────────────────────────────────────────────────
section("Driver leaderboard", "ranked by total rides completed")
st.markdown('<div style="padding: 0 2rem 2rem;">', unsafe_allow_html=True)

df_d = pd.DataFrame(data["drivers"])
if not df_d.empty:
    df_d.columns = ["Driver", "Rating", "Rides", "Revenue ($)", "Avg Tip ($)"]
    df_d["Rank"] = [f"#{i+1}" for i in range(len(df_d))]
    df_d = df_d[["Rank", "Driver", "Rating", "Rides", "Revenue ($)", "Avg Tip ($)"]]
    st.dataframe(df_d, use_container_width=True, hide_index=True)

st.markdown('</div>', unsafe_allow_html=True)
