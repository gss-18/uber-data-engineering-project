"""Compact system status cards."""

from __future__ import annotations

import streamlit as st

from design_tokens import COLORS, FONTS


def render_status_bar(eventhub_live: bool, pipeline_label: str = "DLT Pipeline", ai_ready: bool = False) -> None:
    cards = [
        ("EventHub", "LIVE" if eventhub_live else "OFFLINE", COLORS["status_success"] if eventhub_live else COLORS["status_danger"]),
        ("Pipeline", pipeline_label, COLORS["status_info"]),
        ("AI Analyst", "READY" if ai_ready else "DISABLED", COLORS["status_success"] if ai_ready else COLORS["status_warning"]),
        ("Serving", "Databricks SQL", COLORS["accent_blue"]),
    ]
    cols = st.columns(len(cards))
    for col, (label, value, color) in zip(cols, cards):
        with col:
            st.markdown(
                f"""
<div class="status-card" style="padding:14px 15px;min-height:74px;transition:transform 180ms ease,border-color 180ms ease;">
    <div style="font-family:{FONTS['mono']};font-size:0.55rem;color:{COLORS['text_muted']};letter-spacing:0.14em;text-transform:uppercase;">{label}</div>
    <div style="display:flex;align-items:center;gap:9px;margin-top:10px;">
        <div style="width:8px;height:8px;border-radius:50%;background:{color};box-shadow:0 0 12px {color};"></div>
        <div style="font-family:{FONTS['mono']};font-size:0.78rem;font-weight:700;color:{color};letter-spacing:0.08em;overflow-wrap:anywhere;">{value}</div>
    </div>
</div>
""",
                unsafe_allow_html=True,
            )
