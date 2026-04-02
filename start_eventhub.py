import subprocess
import json
import os
import requests
from dotenv import load_dotenv
load_dotenv()

RESOURCE_GROUP = "rg-uber"
NAMESPACE      = "rg-uber-events"
EVENTHUB       = "ubertopic"
LOCATION       = "eastus"
ENV_FILE       = ".env"

# These scripts run locally — always read from .env via os.getenv()
# st.secrets is only available inside a running Streamlit app, not terminal scripts
DATABRICKS_HOST      = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN     = os.getenv("DATABRICKS_TOKEN")
PIPELINE_ID          = os.getenv("PIPELINE_ID")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
ADMIN_USERNAME       = os.getenv("ADMIN_USERNAME")
ADMIN_PASSWORD       = os.getenv("ADMIN_PASSWORD")
SECRET_KEY           = os.getenv("SECRET_KEY")
STREAMLIT_TOKEN      = os.getenv("STREAMLIT_TOKEN")
STREAMLIT_APP_ID     = os.getenv("STREAMLIT_APP_ID")

HEADERS = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}


def run(cmd):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        exit(1)
    return result.stdout.strip()


def get_pipeline_settings():
    response = requests.get(
        f"{DATABRICKS_HOST}/api/2.0/pipelines/{PIPELINE_ID}",
        headers=HEADERS
    )
    if response.status_code != 200:
        print(f"Failed to get pipeline settings: {response.text}")
        exit(1)
    return response.json()


def update_pipeline_connection_string(listener_conn_str):
    print("Fetching current pipeline settings from Databricks...")
    pipeline = get_pipeline_settings()

    config = pipeline.get("spec", {}).get("configuration", {})
    config["connection_string"] = listener_conn_str

    updated_spec = pipeline["spec"]
    updated_spec["configuration"] = config

    print("Updating connection_string in Databricks pipeline...")
    response = requests.put(
        f"{DATABRICKS_HOST}/api/2.0/pipelines/{PIPELINE_ID}",
        headers=HEADERS,
        json=updated_spec
    )
    if response.status_code != 200:
        print(f"Failed to update pipeline: {response.text}")
        exit(1)
    print("Pipeline configuration updated successfully")


def full_refresh_pipeline():
    print("Triggering full refresh on Databricks pipeline...")
    response = requests.post(
        f"{DATABRICKS_HOST}/api/2.0/pipelines/{PIPELINE_ID}/updates",
        headers=HEADERS,
        json={"full_refresh": True}
    )
    if response.status_code != 200:
        print(f"Failed to trigger full refresh: {response.text}")
        exit(1)
    print("Full refresh triggered successfully")


def restart_pipeline():
    print("Restarting Databricks pipeline (no full refresh)...")
    response = requests.post(
        f"{DATABRICKS_HOST}/api/2.0/pipelines/{PIPELINE_ID}/updates",
        headers=HEADERS,
        json={"full_refresh": False}
    )
    if response.status_code != 200:
        print(f"Failed to restart pipeline: {response.text}")
        exit(1)
    print("Pipeline restarted successfully")


def update_streamlit_secrets(sender_conn, listener_conn):
    """Push new EventHub connection strings to Streamlit Cloud secrets."""
    if not STREAMLIT_TOKEN or not STREAMLIT_APP_ID:
        print("⚠️  STREAMLIT_TOKEN or STREAMLIT_APP_ID not set in .env")
        print("   Add them to .env to enable auto-sync with Streamlit Cloud.")
        print("   Otherwise update CONNECTION_STRING manually in Streamlit Cloud → Settings → Secrets.")
        return

    secrets_toml = f"""CONNECTION_STRING = "{sender_conn}"
LISTENER_CONNECTION_STRING = "{listener_conn}"
EVENT_HUBNAME = "{EVENTHUB}"
DATABRICKS_HOST = "{DATABRICKS_HOST}"
DATABRICKS_TOKEN = "{DATABRICKS_TOKEN}"
PIPELINE_ID = "{PIPELINE_ID}"
DATABRICKS_HTTP_PATH = "{DATABRICKS_HTTP_PATH}"
ADMIN_USERNAME = "{ADMIN_USERNAME}"
ADMIN_PASSWORD = "{ADMIN_PASSWORD}"
SECRET_KEY = "{SECRET_KEY}"
"""

    response = requests.patch(
        f"https://api.streamlit.io/v1/apps/{STREAMLIT_APP_ID}/secrets",
        headers={
            "Authorization": f"Bearer {STREAMLIT_TOKEN}",
            "Content-Type": "application/json"
        },
        json={"secrets": secrets_toml}
    )

    if response.status_code == 200:
        print("✅ Streamlit Cloud secrets updated — app will reload automatically")
    else:
        print(f"⚠️  Streamlit secrets update failed ({response.status_code}): {response.text}")
        print("   Update CONNECTION_STRING manually in Streamlit Cloud → Settings → Secrets.")


# ── Azure EventHub setup ──────────────────────────────────────────
print("Creating EventHub namespace...")
run(f"az eventhubs namespace create --name {NAMESPACE} --resource-group {RESOURCE_GROUP} --location {LOCATION} --sku Standard")

print("Creating EventHub topic...")
run(f"az eventhubs eventhub create --name {EVENTHUB} --namespace-name {NAMESPACE} --resource-group {RESOURCE_GROUP}")

print("Creating SenderPolicy...")
run(f"az eventhubs namespace authorization-rule create --name SenderPolicy --namespace-name {NAMESPACE} --resource-group {RESOURCE_GROUP} --rights Send")

print("Creating ListenerPolicy...")
run(f"az eventhubs namespace authorization-rule create --name ListenerPolicy --namespace-name {NAMESPACE} --resource-group {RESOURCE_GROUP} --rights Listen")

print("Fetching connection strings...")
sender   = json.loads(run(f"az eventhubs namespace authorization-rule keys list --name SenderPolicy --namespace-name {NAMESPACE} --resource-group {RESOURCE_GROUP}"))
listener = json.loads(run(f"az eventhubs namespace authorization-rule keys list --name ListenerPolicy --namespace-name {NAMESPACE} --resource-group {RESOURCE_GROUP}"))

sender_conn  = sender["primaryConnectionString"]
listener_conn = listener["primaryConnectionString"]

# ── Update local .env ─────────────────────────────────────────────
env_contents = f"""CONNECTION_STRING={sender_conn}
EVENT_HUBNAME={EVENTHUB}
LISTENER_CONNECTION_STRING={listener_conn}

DATABRICKS_HOST={DATABRICKS_HOST}
DATABRICKS_TOKEN={DATABRICKS_TOKEN}
PIPELINE_ID={PIPELINE_ID}
DATABRICKS_HTTP_PATH={DATABRICKS_HTTP_PATH}

ADMIN_USERNAME={ADMIN_USERNAME}
ADMIN_PASSWORD={ADMIN_PASSWORD}
SECRET_KEY={SECRET_KEY}

STREAMLIT_TOKEN={STREAMLIT_TOKEN}
STREAMLIT_APP_ID={STREAMLIT_APP_ID}
"""
with open(ENV_FILE, "w") as f:
    f.write(env_contents)
print(".env updated")

# ── Sync new connection strings to Streamlit Cloud ────────────────
update_streamlit_secrets(sender_conn, listener_conn)

# ── Update Databricks pipeline config with new connection string ──
update_pipeline_connection_string(listener_conn)

print("\n" + "=" * 60)
print("ALL DONE")
print(f"  EventHub  : {NAMESPACE} is LIVE")
print(f"  Topic     : {EVENTHUB}")
print(f"  .env      : updated with new connection strings")
print(f"  Streamlit : secrets synced (or see warning above)")
print(f"  Databricks: pipeline config updated (trigger manually from control panel)")
print("=" * 60)
print("\nRemember to run stop_eventhub.py when done to avoid charges!")