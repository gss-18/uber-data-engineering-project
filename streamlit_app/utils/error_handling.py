"""Small UI helpers for graceful Streamlit errors."""

from __future__ import annotations

from functools import wraps
from typing import Callable, TypeVar

import streamlit as st

T = TypeVar("T")


def handle_errors(label: str):
    def decorator(func: Callable[..., T]) -> Callable[..., T | None]:
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                st.error(f"{label} failed: {type(exc).__name__}: {exc}")
                return None

        return wrapper

    return decorator


def show_empty_state(message: str) -> None:
    st.info(message)
