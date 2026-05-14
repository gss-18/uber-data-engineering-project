"""Accessibility helpers for consistent semantic HTML."""

from __future__ import annotations


def accessible_heading(level: int, text: str, subtext: str = "") -> str:
    level = max(1, min(level, 6))
    subtitle = f'<p style="margin:4px 0 0;color:rgba(255,255,255,0.42);font-size:13px;">{subtext}</p>' if subtext else ""
    return (
        f'<h{level} style="margin:0;color:white;font-weight:700;letter-spacing:0;">'
        f"{text}</h{level}>{subtitle}"
    )
