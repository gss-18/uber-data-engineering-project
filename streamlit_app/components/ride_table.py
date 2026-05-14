"""Ride table formatting helpers."""

from __future__ import annotations

import pandas as pd
import streamlit as st


def render_ride_table(df: pd.DataFrame, height: int = 380) -> None:
    if df.empty:
        st.markdown('<div class="empty-state">No ride data available for this section.</div>', unsafe_allow_html=True)
        return

    st.markdown(
        f"""
<div class="table-shell">
    <div class="table-shell-header">
        <div>
            <div class="table-shell-title">Live operations table</div>
            <div class="responsive-table-note">Sortable, sanitized dashboard view</div>
        </div>
        <div class="table-shell-count">{len(df):,} records</div>
    </div>
</div>
""",
        unsafe_allow_html=True,
    )
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        height=height,
    )
