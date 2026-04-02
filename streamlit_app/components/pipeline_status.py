import streamlit as st
import requests as req
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
load_dotenv(override=True)

DATABRICKS_TOKEN = st.secrets.get("DATABRICKS_TOKEN") or os.getenv("DATABRICKS_TOKEN")
DATABRICKS_HOST  = st.secrets.get("DATABRICKS_HOST") or os.getenv("DATABRICKS_HOST")
PIPELINE_ID      = st.secrets.get("PIPELINE_ID") or os.getenv("PIPELINE_ID")

STATE_CONFIG = {
    "RUNNING":      {"color": "#00ffc8", "label": "Running",      "animate": True},
    "STARTING":     {"color": "#00b8ff", "label": "Starting",     "animate": True},
    "INITIALIZING": {"color": "#00b8ff", "label": "Initializing", "animate": True},
    "RESETTING":    {"color": "#ffb800", "label": "Resetting",    "animate": True},
    "STOPPING":     {"color": "#ffb800", "label": "Stopping",     "animate": True},
    "COMPLETED":    {"color": "#00ffc8", "label": "Completed",    "animate": False},
    "FAILED":       {"color": "#ff4444", "label": "Failed",       "animate": False},
    "CANCELED":     {"color": "#888888", "label": "Canceled",     "animate": False},
    "IDLE":         {"color": "rgba(255,255,255,0.2)", "label": "Idle",    "animate": False},
    "UNKNOWN":      {"color": "rgba(255,255,255,0.2)", "label": "Unknown", "animate": False},
}

ACTIVE_PIPELINE_STATES = {"RUNNING", "STARTING", "INITIALIZING", "RESETTING", "STOPPING"}


def get_pipeline_status() -> dict:
    try:
        resp = req.get(
            f"{DATABRICKS_HOST}/api/2.0/pipelines/{PIPELINE_ID}",
            headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
            timeout=5
        )
        if resp.status_code == 200:
            data         = resp.json()
            pipeline_state = data.get("state", "UNKNOWN")
            latest       = data.get("latest_updates", [])
            latest       = latest[0] if latest else {}
            last_modified = data.get("last_modified", 0)  # Unix ms — when pipeline last changed state

            return {
                "pipeline_state":  pipeline_state,
                "update_state":    latest.get("state", ""),
                "creation_time":   latest.get("creation_time", ""),   # ISO — run start time
                "last_modified":   last_modified,                      # Unix ms — run end time
                "full_refresh":    latest.get("full_refresh", False),
            }
    except Exception:
        pass
    return {
        "pipeline_state": "UNKNOWN",
        "update_state":   "",
        "creation_time":  "",
        "last_modified":  0,
        "full_refresh":   False,
    }


def _parse_iso(iso_str: str):
    """Parse ISO 8601 string to datetime."""
    if not iso_str:
        return None
    try:
        return datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    except Exception:
        return None


def _parse_creation_time(creation_time) -> datetime | None:
    """Parse creation_time from Databricks — Unix ms int or ISO string."""
    if not creation_time:
        return None
    if isinstance(creation_time, (int, float)):
        try:
            return datetime.fromtimestamp(creation_time / 1000, tz=timezone.utc)
        except Exception:
            return None
    return _parse_iso(str(creation_time))


def _fmt_duration(seconds: int) -> str:
    mins = seconds // 60
    secs = seconds % 60
    return f"{mins}m {secs}s"


@st.fragment(run_every=1)
def render_pipeline_bar():
    status         = get_pipeline_status()
    pipeline_state = status["pipeline_state"]
    update_state   = status["update_state"]
    full_refresh   = status["full_refresh"]
    creation_time  = status["creation_time"]
    last_modified  = status["last_modified"]

    # ── Determine display state ────────────────────────────────────
    if pipeline_state in ACTIVE_PIPELINE_STATES:
        display_state = pipeline_state          # actively running
    elif update_state in STATE_CONFIG:
        display_state = update_state            # show last run result (COMPLETED/FAILED)
    else:
        display_state = pipeline_state          # IDLE / UNKNOWN

    cfg       = STATE_CONFIG.get(display_state, STATE_CONFIG["UNKNOWN"])
    is_active = cfg["animate"]
    color     = cfg["color"]
    label     = cfg["label"]
    run_type  = "Full Refresh" if full_refresh else "Incremental Update"

    # ── Elapsed / duration time ────────────────────────────────────
    time_str = ""
    start_dt = _parse_creation_time(creation_time)

    if is_active and start_dt:
        # Pipeline is running — show elapsed time since start
        elapsed = int((datetime.now(timezone.utc) - start_dt).total_seconds())
        if elapsed >= 0:
            time_str = f"&#x23F1; {_fmt_duration(elapsed)} elapsed"

    elif not is_active and start_dt and last_modified:
        # Pipeline finished — show how long it took
        end_dt   = datetime.fromtimestamp(last_modified / 1000, tz=timezone.utc)
        duration = int((end_dt - start_dt).total_seconds())
        if 0 < duration < 86400:  # sanity check — ignore if > 1 day
            time_str = f"&#x2713; completed in {_fmt_duration(duration)}"

    # ── HTML assembly ──────────────────────────────────────────────
    dot_shadow   = f"0 0 8px {color}" if is_active else "none"
    pulse_anim   = "animation:pulse 1.5s infinite;" if is_active else ""

    time_html = (
        f'<div style="font-size:0.55rem;color:rgba(255,255,255,0.3);">{time_str}</div>'
        if time_str else ""
    )
    run_html = (
        f'<div style="font-size:0.55rem;color:rgba(255,255,255,0.2);">// {run_type}</div>'
        if is_active else ""
    )

    if is_active:
        bar_inner = (
            f'<div style="height:100%;width:30%;background:{color};'
            f'border-radius:1px;animation:scan 2s linear infinite;"></div>'
        )
    else:
        bar_inner = (
            f'<div style="height:100%;width:100%;background:{color};'
            f'opacity:0.25;border-radius:1px;"></div>'
        )

    dot          = f'<div style="width:6px;height:6px;border-radius:50%;background:{color};box-shadow:{dot_shadow};{pulse_anim}"></div>'
    status_label = f'<div style="font-size:0.65rem;font-weight:700;color:{color};letter-spacing:0.1em;">{label.upper()}</div>'
    bar          = f'<div style="flex:1;height:2px;background:rgba(255,255,255,0.06);border-radius:1px;overflow:hidden;">{bar_inner}</div>'
    pid          = f'<div style="font-size:0.55rem;color:rgba(255,255,255,0.15);white-space:nowrap;">{PIPELINE_ID[:8]}...{PIPELINE_ID[-4:]}</div>'

    html = (
        '<div style="display:flex;align-items:center;gap:1.5rem;padding:0.6rem 2rem;'
        'background:rgba(0,0,0,0.25);border-bottom:1px solid rgba(255,255,255,0.04);'
        'font-family:Space Mono,monospace;">'
        '<div style="font-size:0.55rem;color:rgba(255,255,255,0.25);letter-spacing:0.15em;'
        'text-transform:uppercase;white-space:nowrap;">DLT Pipeline</div>'
        f'<div style="display:flex;align-items:center;gap:0.4rem;">{dot}{status_label}</div>'
        f'{run_html}{time_html}{bar}{pid}'
        '</div>'
        '<style>'
        '@keyframes scan{0%{transform:translateX(-100%)}100%{transform:translateX(400%)}}'
        '@keyframes pulse{0%,100%{opacity:1}50%{opacity:0.3}}'
        '</style>'
    )
    st.markdown(html, unsafe_allow_html=True)
