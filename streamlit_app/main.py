import os
import pathlib
import sys

import streamlit as st
from dotenv import load_dotenv

load_dotenv(override=True)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

APP_DIR = str(pathlib.Path(__file__).parent)
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)

COMPONENTS_DIR = str(pathlib.Path(__file__).parent / "components")
if COMPONENTS_DIR not in sys.path:
    sys.path.insert(0, COMPONENTS_DIR)

from ai_service import is_configured as ai_is_configured
from components.floating_chat import render_floating_chat
from components.pipeline_status import render_pipeline_bar
from components.scroll_animations import inject_scroll_animations
from components.status_bar import render_status_bar
from config_utils import get_secret
from design_tokens import inject_design_system, render_app_header


st.set_page_config(
    page_title="Uber DE - Mission Control",
    page_icon="UB",
    layout="wide",
    initial_sidebar_state="collapsed",
)

inject_design_system()
inject_scroll_animations()

if "warehouse_started" not in st.session_state:
    st.session_state.warehouse_started = True
    try:
        from db import _ensure_warehouse_running

        _ensure_warehouse_running()
    except Exception:
        pass

if "eventhub_live" not in st.session_state:
    try:
        from eventhub_manager import get_connection_strings

        conn_str, _ = get_connection_strings()
        st.session_state.eventhub_live = bool(conn_str)
    except Exception:
        conn_str = get_secret("CONNECTION_STRING")
        st.session_state.eventhub_live = bool(conn_str)

eventhub_live = st.session_state.eventhub_live
ai_ready = ai_is_configured()

render_app_header(eventhub_live=eventhub_live, ai_ready=ai_ready)
render_pipeline_bar()

st.markdown('<div class="app-section" style="padding-top:1rem;">', unsafe_allow_html=True)
render_status_bar(eventhub_live=eventhub_live, ai_ready=ai_ready)
st.markdown("</div>", unsafe_allow_html=True)

tab_analytics, tab_control = st.tabs(
    [
        "01 - Analytics & Live Data",
        "02 - Pipeline Control",
    ]
)

pages_dir = pathlib.Path(__file__).parent / "components"

with tab_analytics:
    exec(open(pages_dir / "analytics.py", encoding="utf-8").read())

with tab_control:
    exec(open(pages_dir / "control.py", encoding="utf-8").read())

render_floating_chat()
