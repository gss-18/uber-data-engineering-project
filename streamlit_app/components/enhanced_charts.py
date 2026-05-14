"""Production-styled Plotly chart helpers."""

from __future__ import annotations

import plotly.graph_objects as go

from design_tokens import CHART_COLORS, COLORS, FONTS


BASE_LAYOUT = dict(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    font=dict(family=FONTS["mono"], color="rgba(255,255,255,0.66)", size=10),
    margin=dict(l=6, r=8, t=46, b=8),
    hoverlabel=dict(bgcolor="#0d1117", font_family="IBM Plex Mono, monospace", font_size=11),
)


def enhanced_bar_chart(df, x_col: str, y_col: str, title: str, color: str = COLORS["accent_cyan"]) -> go.Figure:
    text_values = [_format_compact(value) for value in df[y_col]]
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=df[x_col],
            y=df[y_col],
            marker=dict(color=color, opacity=0.92, line=dict(color="rgba(255,255,255,0.08)", width=1)),
            text=text_values,
            textposition="outside",
            textfont=dict(size=9, family=FONTS["mono"], color=COLORS["text_secondary"]),
            cliponaxis=False,
            hovertemplate="<b>%{x}</b><br>Value: %{y:,}<extra></extra>",
        )
    )
    fig.update_layout(
        **BASE_LAYOUT,
        height=300,
        title=dict(text=title.upper(), font=dict(size=12, family=FONTS["mono"], color=COLORS["text_secondary"]), x=0),
        xaxis=dict(gridcolor="rgba(255,255,255,0.035)", linecolor=COLORS["border_secondary"], tickfont=dict(size=9), automargin=True),
        yaxis=dict(gridcolor="rgba(255,255,255,0.055)", linecolor=COLORS["border_secondary"], tickfont=dict(size=9), rangemode="tozero"),
        showlegend=False,
    )
    return fig


def enhanced_pie_chart(df, names_col: str, values_col: str, title: str) -> go.Figure:
    fig = go.Figure(
        go.Pie(
            labels=df[names_col],
            values=df[values_col],
            hole=0.62,
            marker=dict(colors=CHART_COLORS),
            textinfo="percent",
            textfont=dict(family=FONTS["mono"], size=9),
            hovertemplate="<b>%{label}</b><br>%{value:,} (%{percent})<extra></extra>",
        )
    )
    fig.update_layout(
        **BASE_LAYOUT,
        height=300,
        title=dict(text=title.upper(), font=dict(size=12, family=FONTS["mono"], color=COLORS["text_secondary"]), x=0),
        legend=dict(orientation="v", x=1.02, y=0.5, font=dict(size=9, family=FONTS["mono"]), bgcolor="rgba(0,0,0,0)"),
    )
    return fig


def revenue_region_chart(df) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            name="Revenue",
            x=df["region"],
            y=df["total_revenue"],
            marker=dict(color=COLORS["accent_cyan"], opacity=0.92, line=dict(color="rgba(255,255,255,0.08)", width=1)),
            text=[_format_compact(value) for value in df["total_revenue"]],
            textposition="outside",
            textfont=dict(size=9, family=FONTS["mono"], color=COLORS["text_secondary"]),
            hovertemplate="<b>%{x}</b><br>Revenue: $%{y:,.0f}<extra></extra>",
            yaxis="y1",
        )
    )
    fig.add_trace(
        go.Scatter(
            name="Rides",
            x=df["region"],
            y=df["total_rides"],
            mode="markers+lines",
            marker=dict(color=COLORS["accent_gold"], size=8),
            line=dict(color=COLORS["accent_gold"], width=2, dash="dot"),
            hovertemplate="<b>%{x}</b><br>Rides: %{y:,}<extra></extra>",
            yaxis="y2",
        )
    )
    fig.update_layout(
        **BASE_LAYOUT,
        height=310,
        title=dict(text="REVENUE AND RIDES BY REGION", font=dict(size=12, family=FONTS["mono"], color=COLORS["text_secondary"]), x=0),
        legend=dict(orientation="h", x=0, y=1.14, font=dict(size=9, family=FONTS["mono"]), bgcolor="rgba(0,0,0,0)"),
        xaxis=dict(linecolor=COLORS["border_secondary"], tickfont=dict(size=9)),
        yaxis=dict(title="revenue ($)", gridcolor="rgba(255,255,255,0.05)", tickfont=dict(size=9)),
        yaxis2=dict(title="rides", overlaying="y", side="right", gridcolor="rgba(0,0,0,0)", tickfont=dict(size=9)),
    )
    return fig


def _format_compact(value) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)
    if abs(numeric) >= 1_000_000:
        return f"{numeric / 1_000_000:.1f}M"
    if abs(numeric) >= 1_000:
        return f"{numeric / 1_000:.1f}K"
    if numeric.is_integer():
        return f"{int(numeric)}"
    return f"{numeric:.1f}"
