"""Reusable KPI card components."""

from __future__ import annotations

import hashlib
from typing import Any

import plotly.graph_objects as go
import streamlit as st

from design_tokens import CHART_COLORS, COLORS, FONTS


def render_kpi_card(
    title: str,
    value: str,
    unit: str = "",
    icon: str = "",
    accent: str = COLORS["accent_cyan"],
    sparkline_data: list[float] | None = None,
    metadata: str = "Live aggregate",
    change: str = "",
) -> None:
    change_html = (
        f'<div style="font-family:{FONTS["mono"]};font-size:0.62rem;color:{accent};letter-spacing:0.08em;">{change}</div>'
        if change
        else ""
    )
    st.markdown(
        f"""
<div class="metric-card" style="--metric-accent:{accent};">
    <div style="display:flex;justify-content:space-between;gap:14px;align-items:flex-start;">
        <div>
            <div style="font-family:{FONTS['mono']};font-size:0.56rem;color:{COLORS['text_tertiary']};letter-spacing:0.14em;text-transform:uppercase;">{title}</div>
            <div style="font-family:{FONTS['mono']};font-size:0.52rem;color:{COLORS['text_muted']};margin-top:5px;letter-spacing:0.08em;text-transform:uppercase;">{metadata}</div>
        </div>
        <div style="color:{accent};font-family:{FONTS['mono']};font-size:0.72rem;border:1px solid {accent};background:rgba(255,255,255,0.025);padding:4px 6px;border-radius:4px;">{icon}</div>
    </div>
    <div style="font-family:{FONTS['mono']};font-size:clamp(1.65rem,2.7vw,2.35rem);font-weight:700;color:{COLORS['text_primary']};line-height:1.04;margin-top:18px;overflow-wrap:anywhere;">{value}</div>
    <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;margin-top:8px;">
        <div style="font-family:{FONTS['mono']};font-size:0.62rem;color:{COLORS['text_muted']};letter-spacing:0.06em;text-transform:uppercase;">{unit}</div>
        {change_html}
    </div>
</div>
""",
        unsafe_allow_html=True,
    )

    if sparkline_data and len(sparkline_data) > 1:
        st.plotly_chart(
            create_sparkline(sparkline_data, accent),
            width='stretch',
            config={"displayModeBar": False},
            key=f"sparkline_{_stable_key(title)}",
        )


def render_kpi_grid(metrics: list[dict[str, Any]]) -> None:
    cols = st.columns(len(metrics))
    for idx, (col, metric) in enumerate(zip(cols, metrics)):
        with col:
            render_kpi_card(
                title=metric.get("title", ""),
                value=metric.get("value", "-"),
                unit=metric.get("unit", ""),
                icon=metric.get("icon", ""),
                accent=metric.get("accent", CHART_COLORS[idx % len(CHART_COLORS)]),
                sparkline_data=metric.get("sparkline_data"),
                metadata=metric.get("metadata", "Live aggregate"),
                change=metric.get("change", ""),
            )


def create_sparkline(values: list[float], color: str) -> go.Figure:
    fig = go.Figure(
        go.Scatter(
            y=values,
            mode="lines",
            line=dict(color=color, width=2),
            fill="tozeroy",
            fillcolor="rgba(0,229,195,0.06)",
            hovertemplate="%{y}<extra></extra>",
        )
    )
    fig.update_layout(
        height=54,
        margin=dict(l=0, r=0, t=4, b=0),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        showlegend=False,
    )
    return fig


def _stable_key(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:10]


def render_kpi_cards_html(k: dict) -> None:
    """Render the 7-KPI grid using the mission-control.html design.

    Elements are placed in the parent Streamlit DOM with class .kc so the
    IntersectionObserver in scroll_animations.py can target them for the
    rotateX flip-in and count-up animations.
    """
    def _n(v, fb: float = 0.0) -> float:
        try:
            return float(v) if v is not None else fb
        except (TypeError, ValueError):
            return fb

    CARDS = [
        ("Total Rides",  "RIDES", int(_n(k.get("total_rides",  0))), "",  "",  "int",  "Rides"),
        ("Revenue",      "USD",   int(_n(k.get("total_revenue", 0))), "$", "",  "int",  "USD"),
        ("Avg Fare",     "FARE",  _n(k.get("avg_fare",      0)),      "$", "",  "dec2", "Per Ride"),
        ("Avg Surge",    "SURGE", _n(k.get("avg_surge",     0)),      "",  "x", "dec2", "Multiplier"),
        ("Avg Rating",   "STAR",  _n(k.get("avg_rating",    0)),      "",  "",  "dec2", "/ 5.0"),
        ("Avg Distance", "DIST",  _n(k.get("avg_distance",  0)),      "",  "",  "dec1", "Miles"),
        ("Cancellation", "CXL",   _n(k.get("cancellation_rate", 0)),  "",  "%", "dec1", "Rate"),
    ]

    items = "".join(
        f'<div class="kc" data-t="{t}" data-p="{p}" data-s="{s}" data-f="{f}">'
        f'<div class="kc-top">'
        f'<span class="kc-lbl">{lbl}</span>'
        f'<span class="kc-badge">{badge}</span>'
        f'</div>'
        f'<div class="kc-agg">Live Aggregate</div>'
        f'<span class="kc-val">–</span>'
        f'<span class="kc-sub">{sub}</span>'
        f'</div>'
        for lbl, badge, t, p, s, f, sub in CARDS
    )
    st.markdown(f'<div class="kpi-grid">{items}</div>', unsafe_allow_html=True)
