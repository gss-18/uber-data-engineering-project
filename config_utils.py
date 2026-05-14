import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env", override=True)


def get_secret(key: str, default: str | None = None) -> str | None:
    """Read Streamlit secrets when available, otherwise fall back to environment."""
    try:
        import streamlit as st

        try:
            value = st.secrets[key]
            if value is not None:
                return str(value)
        except Exception:
            pass
    except Exception:
        pass

    return os.getenv(key, default)
