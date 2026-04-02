import streamlit as st
import subprocess
import os
import sys
import time
import requests as req
from dotenv import load_dotenv
load_dotenv(override=True)

ADMIN_USERNAME   = os.getenv("ADMIN_USERNAME")
ADMIN_PASSWORD   = os.getenv("ADMIN_PASSWORD")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_HOST  = os.getenv("DATABRICKS_HOST")
PIPELINE_ID      = os.getenv("PIPELINE_ID")
if 'PROJECT_ROOT' not in vars():
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) if "__file__" in dir() else os.path.dirname(os.path.dirname(os.getcwd()))


def section(label, sublabel=""):
    st.markdown(f"""
    <div style="display:flex;align-items:center;gap:1rem;padding:1.5rem 2rem 0.75rem;border-top:1px solid rgba(255,255,255,0.04);">
        <div style="font-family:'Space Mono',monospace;font-size:0.6rem;color:#ffb800;letter-spacing:0.2em;text-transform:uppercase;">{label}</div>
        {'<div style="font-family:Space Mono,monospace;font-size:0.55rem;color:rgba(255,255,255,0.2);letter-spacing:0.1em;">// ' + sublabel + '</div>' if sublabel else ''}
    </div>
    """, unsafe_allow_html=True)


# ── Auth ───────────────────────────────────────────────────────────
if "control_auth" not in st.session_state:
    st.session_state.control_auth = False

if not st.session_state.control_auth:
    _, col_form, _ = st.columns([1, 1, 1])
    with col_form:
        st.markdown('<div style="padding: 4rem 0 2rem;">', unsafe_allow_html=True)
        st.markdown("""
        <div style="font-family:'Space Mono',monospace;font-size:0.6rem;color:rgba(255,255,255,0.3);letter-spacing:0.2em;text-transform:uppercase;margin-bottom:2rem;text-align:center;">
            // Authentication required to access pipeline controls
        </div>
        """, unsafe_allow_html=True)
        with st.form("auth_form"):
            st.markdown("""
            <div style="font-family:'Space Mono',monospace;font-size:0.65rem;color:rgba(255,255,255,0.4);letter-spacing:0.15em;margin-bottom:1rem;">
                PIPELINE ACCESS
            </div>
            """, unsafe_allow_html=True)
            username  = st.text_input("Username", placeholder="username")
            password  = st.text_input("Password", type="password", placeholder="••••••••")
            submitted = st.form_submit_button("Authenticate →", type="primary", use_container_width=True)
            if submitted:
                if username == ADMIN_USERNAME and password == ADMIN_PASSWORD:
                    st.session_state.control_auth = True
                    st.rerun()
                else:
                    st.error("Access denied")
        st.markdown('</div>', unsafe_allow_html=True)
    st.stop()

# ── Authenticated ──────────────────────────────────────────────────
load_dotenv(override=True)
connection_string = os.getenv("CONNECTION_STRING")
eventhub_live     = bool(connection_string)

# ── Status header ──────────────────────────────────────────────────
st.markdown(f"""
<div style="display:flex;align-items:center;justify-content:space-between;padding:1.5rem 2rem;background:rgba(0,0,0,0.2);border-bottom:1px solid rgba(255,255,255,0.04);">
    <div style="display:flex;align-items:center;gap:2rem;">
        <div>
            <div style="font-family:'Space Mono',monospace;font-size:0.55rem;color:rgba(255,255,255,0.3);letter-spacing:0.15em;text-transform:uppercase;margin-bottom:0.3rem;">EventHub Status</div>
            <div style="display:flex;align-items:center;gap:0.5rem;">
                <div style="width:8px;height:8px;border-radius:50%;background:{'#00ffc8' if eventhub_live else '#ff4444'};box-shadow:0 0 {'10px #00ffc8' if eventhub_live else '10px #ff4444'};"></div>
                <div style="font-family:'Space Mono',monospace;font-size:0.8rem;font-weight:700;color:{'#00ffc8' if eventhub_live else '#ff4444'};">{'LIVE' if eventhub_live else 'OFFLINE'}</div>
            </div>
        </div>
        <div>
            <div style="font-family:'Space Mono',monospace;font-size:0.55rem;color:rgba(255,255,255,0.3);letter-spacing:0.15em;text-transform:uppercase;margin-bottom:0.3rem;">Namespace</div>
            <div style="font-family:'Space Mono',monospace;font-size:0.75rem;color:rgba(255,255,255,0.7);">rg-uber-events</div>
        </div>
        <div>
            <div style="font-family:'Space Mono',monospace;font-size:0.55rem;color:rgba(255,255,255,0.3);letter-spacing:0.15em;text-transform:uppercase;margin-bottom:0.3rem;">Topic</div>
            <div style="font-family:'Space Mono',monospace;font-size:0.75rem;color:rgba(255,255,255,0.7);">ubertopic</div>
        </div>
        <div>
            <div style="font-family:'Space Mono',monospace;font-size:0.55rem;color:rgba(255,255,255,0.3);letter-spacing:0.15em;text-transform:uppercase;margin-bottom:0.3rem;">Workspace</div>
            <div style="font-family:'Space Mono',monospace;font-size:0.75rem;color:rgba(255,255,255,0.7);">dbc-fcbf72bc</div>
        </div>
    </div>
    <div style="font-family:'Space Mono',monospace;font-size:0.6rem;color:rgba(255,255,255,0.2);">Operator: {ADMIN_USERNAME}</div>
</div>
""", unsafe_allow_html=True)

# ── EventHub controls ──────────────────────────────────────────────
section("EventHub control", "create or destroy the streaming namespace")
st.markdown('<div style="padding: 0 2rem 1.5rem;">', unsafe_allow_html=True)

col_start, col_stop, col_info = st.columns([1, 1, 3])

with col_start:
    if st.button("▶ Start EventHub", type="primary", disabled=eventhub_live, use_container_width=True):
        with st.spinner("Provisioning EventHub namespace (~2 min)..."):
            result = subprocess.run(
                ["python", "start_eventhub.py"],
                capture_output=True, text=True, encoding="utf-8", cwd=PROJECT_ROOT
            )
        if result.returncode == 0:
            st.success("EventHub started")
            st.code(result.stdout, language=None)
        else:
            st.error("Failed to start EventHub")
            st.code(result.stderr, language=None)
        time.sleep(1)
        st.rerun()

with col_stop:
    confirm = st.checkbox("Confirm stop", disabled=not eventhub_live)
    if st.button("⏹ Stop EventHub", type="secondary", disabled=(not eventhub_live or not confirm), use_container_width=True):
        with st.spinner("Deleting namespace — billing stops immediately"):
            result = subprocess.run(
                ["python", "stop_eventhub.py", "--yes"],
                capture_output=True, text=True, encoding="utf-8", cwd=PROJECT_ROOT
            )
        if result.returncode == 0:
            st.success("EventHub deleted — billing stopped")
        else:
            st.error("Failed to stop EventHub")
            st.code(result.stderr, language=None)
        time.sleep(1)
        st.rerun()

with col_info:
    st.markdown("""
    <div style="border:1px solid rgba(255,184,0,0.15);background:rgba(255,184,0,0.04);padding:1rem 1.25rem;border-radius:2px;font-family:'Space Mono',monospace;font-size:0.65rem;color:rgba(255,255,255,0.35);line-height:1.8;">
        <span style="color:rgba(255,184,0,0.7);">// cost model</span><br>
        Standard tier · ~$0.015/TU/hr · delete when not in use<br>
        Start: creates namespace + policies + updates .env + triggers pipeline<br>
        Stop: deletes namespace + clears credentials + preserves archive data
    </div>
    """, unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)

# ── Pipeline control ───────────────────────────────────────────────
section("Databricks DLT pipeline", "trigger updates and monitor pipeline status")
st.markdown('<div style="padding: 0 2rem 1.5rem;">', unsafe_allow_html=True)

col_trigger, col_full, col_pinfo = st.columns([1, 1, 3])

with col_trigger:
    if st.button("⚡ Trigger Update", type="primary", use_container_width=True):
        resp = req.post(
            f"{DATABRICKS_HOST}/api/2.0/pipelines/{PIPELINE_ID}/updates",
            headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
            json={"full_refresh": False}
        )
        if resp.status_code == 200:
            st.success("Pipeline update triggered")
        else:
            st.error(f"Failed: {resp.text}")

with col_full:
    confirm_full = st.checkbox("Confirm full refresh")
    if st.button("↺ Full Refresh", type="secondary", disabled=not confirm_full, use_container_width=True):
        resp = req.post(
            f"{DATABRICKS_HOST}/api/2.0/pipelines/{PIPELINE_ID}/updates",
            headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
            json={"full_refresh": True}
        )
        if resp.status_code == 200:
            st.success("Full refresh triggered")
        else:
            st.error(f"Failed: {resp.text}")

with col_pinfo:
    st.markdown(f"""
    <div style="border:1px solid rgba(0,255,200,0.12);background:rgba(0,255,200,0.02);padding:1rem 1.25rem;border-radius:2px;font-family:'Space Mono',monospace;font-size:0.65rem;color:rgba(255,255,255,0.35);line-height:1.8;">
        <span style="color:rgba(0,255,200,0.5);">// pipeline config</span><br>
        ID: {PIPELINE_ID[:8]}...{PIPELINE_ID[-4:]}<br>
        Workspace: {DATABRICKS_HOST.replace('https://','')}<br>
        Mode: Triggered · Compute: Serverless · Catalog: uber
    </div>
    """, unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)

# ── Schema overview ────────────────────────────────────────────────
section("Data architecture", "medallion schema — bronze → silver → gold")
st.markdown('<div style="padding: 0 2rem 1.5rem;">', unsafe_allow_html=True)

col_b, col_s, col_g = st.columns(3)

schemas = {
    "Bronze": {
        "color": "#cd7f32",
        "tables": [
            ("bulk_rides",               "2,000 historical rides"),
            ("rides_raw",                "live EventHub stream"),
            ("streaming_rides_archive",  "persistent stream backup"),
            ("map_cities",               "10 US cities"),
            ("map_cancellation_reasons", "4 reason codes"),
            ("map_payment_methods",      "4 payment types"),
            ("map_ride_statuses",        "2 status codes"),
            ("map_vehicle_makes",        "7 manufacturers"),
            ("map_vehicle_types",        "5 service tiers"),
        ]
    },
    "Silver": {
        "color": "#c0c0c0",
        "tables": [
            ("stg_rides",  "merged bulk + stream + archive"),
            ("silver_obt", "one big table with all joins"),
        ]
    },
    "Gold": {
        "color": "#ffd700",
        "tables": [
            ("fact",          "ride measures + FK keys"),
            ("dim_passenger", "SCD1 · passenger profiles"),
            ("dim_driver",    "SCD1 · driver profiles"),
            ("dim_vehicle",   "SCD1 · vehicle registry"),
            ("dim_booking",   "SCD1 · booking details"),
            ("dim_payment",   "SCD1 · payment methods"),
            ("dim_location",  "SCD2 · city dimension"),
        ]
    }
}

for col, (schema_name, schema_data) in zip([col_b, col_s, col_g], schemas.items()):
    with col:
        color = schema_data["color"]
        tables_html = "".join([
            f"""<div style="display:flex;flex-direction:column;gap:2px;padding:0.6rem 0.75rem;border-bottom:1px solid rgba(255,255,255,0.04);">
                <div style="font-family:'Space Mono',monospace;font-size:0.65rem;color:rgba(255,255,255,0.7);">{t[0]}</div>
                <div style="font-family:'Space Mono',monospace;font-size:0.55rem;color:rgba(255,255,255,0.25);">{t[1]}</div>
            </div>"""
            for t in schema_data["tables"]
        ])
        st.markdown(f"""
        <div style="border:1px solid rgba(255,255,255,0.08);border-top:2px solid {color};background:rgba(255,255,255,0.02);border-radius:2px;overflow:hidden;">
            <div style="padding:0.75rem;background:rgba(255,255,255,0.03);border-bottom:1px solid rgba(255,255,255,0.06);font-family:'Space Mono',monospace;font-size:0.65rem;color:{color};letter-spacing:0.1em;">
                {schema_name} · {len(schema_data['tables'])} tables
            </div>
            {tables_html}
        </div>
        """, unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)

# ── Logout ─────────────────────────────────────────────────────────
st.markdown('<div style="padding: 1rem 2rem 2rem; text-align:right;">', unsafe_allow_html=True)
if st.button("Sign out", type="secondary"):
    st.session_state.control_auth = False
    st.rerun()
st.markdown('</div>', unsafe_allow_html=True)