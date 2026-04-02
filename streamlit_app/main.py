import streamlit as st
import sys
import os
import pathlib
from dotenv import load_dotenv
load_dotenv(override=True)

# Inject Streamlit Cloud secrets into os.environ so os.getenv() works everywhere
for _k, _v in st.secrets.items():
    os.environ.setdefault(_k, str(_v))

# ── Path setup ─────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Add components dir so pipeline_status is importable
COMPONENTS_DIR = str(pathlib.Path(__file__).parent / "components")
if COMPONENTS_DIR not in sys.path:
    sys.path.insert(0, COMPONENTS_DIR)

from components.pipeline_status import render_pipeline_bar

# ── Page config — must be first Streamlit call ─────────────────────
st.set_page_config(
    page_title="Uber DE — Mission Control",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ── Global CSS ─────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;700;800&display=swap');

*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

html, body, [data-testid="stAppViewContainer"] {
    background: #080c10 !important;
    font-family: 'Syne', sans-serif;
}
[data-testid="stAppViewContainer"] {
    background:
        linear-gradient(rgba(0,255,200,0.03) 1px, transparent 1px),
        linear-gradient(90deg, rgba(0,255,200,0.03) 1px, transparent 1px),
        #080c10 !important;
    background-size: 40px 40px !important;
}
[data-testid="stSidebar"] { display: none !important; }
[data-testid="collapsedControl"] { display: none !important; }
header[data-testid="stHeader"] { background: transparent !important; }
.block-container { padding: 0 !important; max-width: 100% !important; }
footer { display: none !important; }

.stTabs [data-baseweb="tab-list"] {
    background: transparent !important;
    border-bottom: 1px solid rgba(0,255,200,0.15);
    gap: 0;
    padding: 0 2rem;
}
.stTabs [data-baseweb="tab"] {
    font-family: 'Space Mono', monospace !important;
    font-size: 0.75rem !important;
    letter-spacing: 0.15em !important;
    text-transform: uppercase !important;
    color: rgba(255,255,255,0.4) !important;
    padding: 1rem 2rem !important;
    border-bottom: 2px solid transparent !important;
    background: transparent !important;
}
.stTabs [aria-selected="true"] {
    color: #00ffc8 !important;
    border-bottom: 2px solid #00ffc8 !important;
    background: transparent !important;
}
.stTabs [data-baseweb="tab-panel"] {
    padding: 0 !important;
    background: transparent !important;
}

.stMetricLabel { color: rgba(255,255,255,0.4) !important; font-family: 'Space Mono', monospace !important; font-size: 0.65rem !important; letter-spacing: 0.1em !important; text-transform: uppercase !important; }
.stMetricValue { color: #ffffff !important; font-family: 'Space Mono', monospace !important; font-size: 1.6rem !important; font-weight: 700 !important; }

.stDataFrame thead tr th { background: rgba(0,255,200,0.05) !important; color: rgba(255,255,255,0.5) !important; font-family: 'Space Mono', monospace !important; font-size: 0.65rem !important; letter-spacing: 0.1em !important; text-transform: uppercase !important; border-bottom: 1px solid rgba(0,255,200,0.15) !important; }
.stDataFrame tbody tr td { color: rgba(255,255,255,0.8) !important; font-family: 'Space Mono', monospace !important; font-size: 0.75rem !important; border-bottom: 1px solid rgba(255,255,255,0.04) !important; }
.stDataFrame tbody tr:hover td { background: rgba(0,255,200,0.04) !important; }

button[kind="primary"] {
    background: #00ffc8 !important;
    color: #080c10 !important;
    font-family: 'Space Mono', monospace !important;
    font-size: 0.7rem !important;
    font-weight: 700 !important;
    letter-spacing: 0.1em !important;
    text-transform: uppercase !important;
    border: none !important;
    border-radius: 2px !important;
    padding: 0.6rem 1.5rem !important;
}
button[kind="secondary"] {
    background: transparent !important;
    color: rgba(255,255,255,0.6) !important;
    font-family: 'Space Mono', monospace !important;
    font-size: 0.7rem !important;
    letter-spacing: 0.1em !important;
    text-transform: uppercase !important;
    border: 1px solid rgba(255,255,255,0.2) !important;
    border-radius: 2px !important;
}

.stSelectbox label, .stNumberInput label, .stToggle label {
    font-family: 'Space Mono', monospace !important;
    font-size: 0.65rem !important;
    color: rgba(255,255,255,0.4) !important;
    letter-spacing: 0.1em !important;
    text-transform: uppercase !important;
}
[data-testid="stNumberInputContainer"] input,
[data-baseweb="select"] {
    background: rgba(255,255,255,0.04) !important;
    border: 1px solid rgba(255,255,255,0.1) !important;
    color: white !important;
    font-family: 'Space Mono', monospace !important;
    border-radius: 2px !important;
}
.stCheckbox label p {
    font-family: 'Space Mono', monospace !important;
    font-size: 0.7rem !important;
    color: rgba(255,255,255,0.6) !important;
}

.stSuccess { background: rgba(0,255,200,0.08) !important; border: 1px solid rgba(0,255,200,0.3) !important; border-radius: 2px !important; }
.stError   { background: rgba(255,60,60,0.08) !important; border: 1px solid rgba(255,60,60,0.3) !important; border-radius: 2px !important; }
.stWarning { background: rgba(255,180,0,0.08) !important; border: 1px solid rgba(255,180,0,0.3) !important; border-radius: 2px !important; }
.stInfo    { background: rgba(0,180,255,0.08) !important; border: 1px solid rgba(0,180,255,0.3) !important; border-radius: 2px !important; }
.stProgress > div > div { background: #00ffc8 !important; }
</style>
""", unsafe_allow_html=True)

# ── Top bar ────────────────────────────────────────────────────────
connection_string = os.getenv("CONNECTION_STRING")
eventhub_live = bool(connection_string)

st.markdown(f"""
<div style="
    display:flex;align-items:center;justify-content:space-between;
    padding:1rem 2rem;
    border-bottom:1px solid rgba(0,255,200,0.12);
    background:rgba(0,0,0,0.4);
    backdrop-filter:blur(10px);
    position:sticky;top:0;z-index:999;
">
    <div style="display:flex;align-items:center;gap:1.5rem;">
        <div style="font-family:'Space Mono',monospace;font-size:0.65rem;letter-spacing:0.25em;color:rgba(255,255,255,0.3);text-transform:uppercase;">⚡ Uber</div>
        <div style="font-family:'Syne',sans-serif;font-size:1.1rem;font-weight:800;color:white;letter-spacing:-0.01em;">Real-Time Data Engineering</div>
        <div style="font-family:'Space Mono',monospace;font-size:0.6rem;color:rgba(255,255,255,0.2);letter-spacing:0.15em;">MISSION CONTROL</div>
    </div>
    <div style="display:flex;align-items:center;gap:1rem;">
        <div style="display:flex;align-items:center;gap:0.4rem;font-family:'Space Mono',monospace;font-size:0.65rem;color:{'#00ffc8' if eventhub_live else 'rgba(255,80,80,0.8)'};letter-spacing:0.1em;">
            <div style="width:6px;height:6px;border-radius:50%;background:{'#00ffc8' if eventhub_live else '#ff4444'};box-shadow:0 0 {'8px #00ffc8' if eventhub_live else '8px #ff4444'};animation:pulse 2s infinite;"></div>
            EVENTHUB {'LIVE' if eventhub_live else 'OFFLINE'}
        </div>
        <div style="font-family:'Space Mono',monospace;font-size:0.6rem;color:rgba(255,255,255,0.2);">DLT Pipeline · Delta Lake · Azure</div>
    </div>
</div>
<style>
@keyframes pulse {{ 0%, 100% {{ opacity:1; }} 50% {{ opacity:0.4; }} }}
</style>
""", unsafe_allow_html=True)

# ── Pipeline status bar — sits between header and tabs ─────────────
render_pipeline_bar()

# ── Two tabs ───────────────────────────────────────────────────────
tab_analytics, tab_control = st.tabs([
    "01 — Analytics & Live Data",
    "02 — Pipeline Control"
])

pages_dir = pathlib.Path(__file__).parent / "components"

with tab_analytics:
    exec(open(pages_dir / "analytics.py", encoding="utf-8").read())

with tab_control:
    exec(open(pages_dir / "control.py", encoding="utf-8").read())