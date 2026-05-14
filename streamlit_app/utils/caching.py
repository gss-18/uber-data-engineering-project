"""Session-state cache helpers."""

from __future__ import annotations

import time
from functools import wraps
from typing import Callable, TypeVar

import streamlit as st

T = TypeVar("T")


def session_cache(ttl_seconds: int = 60):
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache_key = f"{func.__module__}.{func.__name__}:{args}:{kwargs}"
            now = time.time()
            if cache_key in st.session_state:
                value, timestamp = st.session_state[cache_key]
                if now - timestamp < ttl_seconds:
                    return value
            value = func(*args, **kwargs)
            st.session_state[cache_key] = (value, now)
            return value

        return wrapper

    return decorator
