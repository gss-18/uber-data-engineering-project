import subprocess
import os
import sys
import requests
from dotenv import load_dotenv
load_dotenv()

RESOURCE_GROUP = "rg-uber"
NAMESPACE      = "rg-uber-events"
EVENTHUB       = "ubertopic"
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


def run(cmd):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.stdout.strip()


def update_streamlit_secrets_offline():
    """Clear EventHub connection strings in Streamlit Cloud secrets."""
    if not STREAMLIT_TOKEN or not STREAMLIT_APP_ID:
        print("⚠️  STREAMLIT_TOKEN or STREAMLIT_APP_ID not set in .env")
        print("   Clear CONNECTION_STRING manually in Streamlit Cloud → Settings → Secrets.")
        return

    secrets_toml = f"""CONNECTION_STRING = ""
LISTENER_CONNECTION_STRING = ""
EVENT_HUBNAME = ""
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
        print("✅ Streamlit Cloud secrets cleared — EventHub shown as OFFLINE in app")
    else:
        print(f"⚠️  Streamlit secrets update failed ({response.status_code}): {response.text}")
        print("   Clear CONNECTION_STRING manually in Streamlit Cloud → Settings → Secrets.")


# ── Confirmation ──────────────────────────────────────────────────
auto_confirm = "--yes" in sys.argv

if not auto_confirm:
    confirm = input(f"Delete EventHub namespace '{NAMESPACE}'? This stops billing. (yes/no): ")
    if confirm.lower() != "yes":
        print("Cancelled.")
        exit(0)

# ── Delete EventHub ───────────────────────────────────────────────
print(f"Deleting {NAMESPACE}...")
run(f"az eventhubs namespace delete --name {NAMESPACE} --resource-group {RESOURCE_GROUP}")

# ── Clear local .env ──────────────────────────────────────────────
env_contents = f"""CONNECTION_STRING=
EVENT_HUBNAME=
LISTENER_CONNECTION_STRING=

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
print(".env EventHub credentials cleared")

# ── Sync cleared secrets to Streamlit Cloud ───────────────────────
update_streamlit_secrets_offline()

print("\n" + "=" * 60)
print("EventHub DELETED — billing stopped")
print(".env EventHub credentials cleared, Databricks variables preserved")
print("Streamlit Cloud: EventHub secrets cleared (or see warning above)")
print("=" * 60)