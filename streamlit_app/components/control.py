import streamlit as st
import os
import time
import requests as req
from dotenv import load_dotenv
load_dotenv(override=True)

from eventhub_manager import (
    start_eventhub,
    stop_eventhub,
    update_pipeline_connection_string,
    trigger_pipeline,
)

ADMIN_USERNAME   = st.secrets.get("ADMIN_USERNAME")   or os.getenv("ADMIN_USERNAME")
ADMIN_PASSWORD   = st.secrets.get("ADMIN_PASSWORD")   or os.getenv("ADMIN_PASSWORD")
DATABRICKS_TOKEN = st.secrets.get("DATABRICKS_TOKEN") or os.getenv("DATABRICKS_TOKEN")
DATABRICKS_HOST  = st.secrets.get("DATABRICKS_HOST")  or os.getenv("DATABRICKS_HOST")
PIPELINE_ID      = st.secrets.get("PIPELINE_ID")      or os.getenv("PIPELINE_ID")

EVENTHUB = "ubertopic"


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
connection_string = st.secrets.get("CONNECTION_STRING") or os.getenv("CONNECTION_STRING")
eventhub_live     = bool(connection_string)

# ── Status header ──────────────────────────────────────────────────
st.markdown(f"""
<div style="display:flex;align-items:center;justify-content:space-between;padding:1.5rem 2rem;background:rgba(0,0,0,0.2);border-bottom:1px solid rgba(255,255,255,0.04);">
    <div style="display:flex;align-items:center;gap:2rem;">
        <div>
            <div style="font-family:'Space Mono',monospace;font-size:0.55rem;color:rgba(255,255,255,0.3);letter-spacing:0.15em;text-transform:uppercase;margin-bottom:0.3rem;">EventHub Status</div>
            <div style="display:flex;align-items:center;gap:0.5rem;">
                <div style="width:8px;height:8px;border-radius:50%;background:{'#00ffc8' if eventhub_live else '#ff4444'};box-shadow:0 0 {'10px #00ffc8' if eventhub_live else '10px #ff4444'};"></div>
                <div style="font-family:'Space Mono',monospace;font-size:0.8rem;font-weight:700;color:{'#00ffc8' if eventhub_live else '#ff4444'};font-weight:700;">{'LIVE' if eventhub_live else 'OFFLINE'}</div>
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
            try:
                sender_conn, listener_conn = start_eventhub()
                update_pipeline_connection_string(listener_conn)

                # Build secrets block for manual paste into Streamlit Cloud
                secrets_block = f"""CONNECTION_STRING = "{sender_conn}"
LISTENER_CONNECTION_STRING = "{listener_conn}"
EVENT_HUBNAME = "{EVENTHUB}"
DATABRICKS_HOST = "{DATABRICKS_HOST}"
DATABRICKS_TOKEN = "{DATABRICKS_TOKEN}"
PIPELINE_ID = "{PIPELINE_ID}"
DATABRICKS_HTTP_PATH = "{st.secrets.get('DATABRICKS_HTTP_PATH') or os.getenv('DATABRICKS_HTTP_PATH')}"
ADMIN_USERNAME = "{ADMIN_USERNAME}"
ADMIN_PASSWORD = "{ADMIN_PASSWORD}"
SECRET_KEY = "{st.secrets.get('SECRET_KEY') or os.getenv('SECRET_KEY')}"
AZURE_TENANT_ID = "{st.secrets.get('AZURE_TENANT_ID') or os.getenv('AZURE_TENANT_ID')}"
AZURE_CLIENT_ID = "{st.secrets.get('AZURE_CLIENT_ID') or os.getenv('AZURE_CLIENT_ID')}"
AZURE_CLIENT_SECRET = "{st.secrets.get('AZURE_CLIENT_SECRET') or os.getenv('AZURE_CLIENT_SECRET')}"
AZURE_SUBSCRIPTION_ID = "{st.secrets.get('AZURE_SUBSCRIPTION_ID') or os.getenv('AZURE_SUBSCRIPTION_ID')}"
"""
                st.success("✅ EventHub started — Databricks pipeline config updated")
                st.warning(
                    "⚠️ Copy the secrets block below and paste it into "
                    "**App ⋮ → Settings → Secrets**, then reboot the app "
                    "so the new connection string takes effect."
                )
                st.code(secrets_block, language="toml")

            except Exception as e:
                st.error(f"Failed to start EventHub: {e}")

        time.sleep(1)
        st.rerun()

with col_stop:
    confirm = st.checkbox("Confirm stop", disabled=not eventhub_live)
    if st.button("⏹ Stop EventHub", type="secondary", disabled=(not eventhub_live or not confirm), use_container_width=True):
        with st.spinner("Deleting namespace — billing stops immediately..."):
            try:
                stop_eventhub()

                secrets_block = f"""CONNECTION_STRING = ""
LISTENER_CONNECTION_STRING = ""
EVENT_HUBNAME = ""
DATABRICKS_HOST = "{DATABRICKS_HOST}"
DATABRICKS_TOKEN = "{DATABRICKS_TOKEN}"
PIPELINE_ID = "{PIPELINE_ID}"
DATABRICKS_HTTP_PATH = "{st.secrets.get('DATABRICKS_HTTP_PATH') or os.getenv('DATABRICKS_HTTP_PATH')}"
ADMIN_USERNAME = "{ADMIN_USERNAME}"
ADMIN_PASSWORD = "{ADMIN_PASSWORD}"
SECRET_KEY = "{st.secrets.get('SECRET_KEY') or os.getenv('SECRET_KEY')}"
AZURE_TENANT_ID = "{st.secrets.get('AZURE_TENANT_ID') or os.getenv('AZURE_TENANT_ID')}"
AZURE_CLIENT_ID = "{st.secrets.get('AZURE_CLIENT_ID') or os.getenv('AZURE_CLIENT_ID')}"
AZURE_CLIENT_SECRET = "{st.secrets.get('AZURE_CLIENT_SECRET') or os.getenv('AZURE_CLIENT_SECRET')}"
AZURE_SUBSCRIPTION_ID = "{st.secrets.get('AZURE_SUBSCRIPTION_ID') or os.getenv('AZURE_SUBSCRIPTION_ID')}"
"""
                st.success("✅ EventHub deleted — billing stopped")
                st.warning(
                    "⚠️ Paste the block below into **App ⋮ → Settings → Secrets** "
                    "and reboot to mark EventHub as OFFLINE in the dashboard."
                )
                st.code(secrets_block, language="toml")

            except Exception as e:
                st.error(f"Failed to stop EventHub: {e}")

        time.sleep(1)
        st.rerun()

with col_info:
    st.markdown("""
    <div style="border:1px solid rgba(255,184,0,0.15);background:rgba(255,184,0,0.04);padding:1rem 1.25rem;border-radius:2px;font-family:'Space Mono',monospace;font-size:0.65rem;color:rgba(255,255,255,0.35);line-height:1.8;">
        <span style="color:rgba(255,184,0,0.7);">// cost model</span><br>
        Standard tier · ~$0.015/TU/hr · delete when not in use<br>
        Start: creates namespace + policies + updates Databricks pipeline config<br>
        Stop: deletes namespace + preserves archive data in Delta Lake
    </div>
    """, unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)

# ── Pipeline control ───────────────────────────────────────────────
section("Databricks DLT pipeline", "trigger updates and monitor pipeline status")
st.markdown('<div style="padding: 0 2rem 1.5rem;">', unsafe_allow_html=True)

col_trigger, col_full, col_pinfo = st.columns([1, 1, 3])

with col_trigger:
    if st.button("⚡ Trigger Update", type="primary", use_container_width=True):
        resp = trigger_pipeline(full_refresh=False)
        if resp.status_code == 200:
            st.success("Pipeline update triggered")
        else:
            st.error(f"Failed: {resp.text}")

with col_full:
    confirm_full = st.checkbox("Confirm full refresh")
    if st.button("↺ Full Refresh", type="secondary", disabled=not confirm_full, use_container_width=True):
        resp = trigger_pipeline(full_refresh=True)
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