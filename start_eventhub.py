"""
start_eventhub.py — Local helper script
───────────────────────────────────────
Run this locally to provision EventHub for a new working session.
On Streamlit Cloud, the same logic runs via the control panel button.
"""
import os
from dotenv import load_dotenv
load_dotenv()

from eventhub_manager import start_eventhub, update_pipeline_connection_string

EVENTHUB             = "ubertopic"
ENV_FILE             = ".env"
DATABRICKS_HOST      = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN     = os.getenv("DATABRICKS_TOKEN")
PIPELINE_ID          = os.getenv("PIPELINE_ID")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
ADMIN_USERNAME       = os.getenv("ADMIN_USERNAME")
ADMIN_PASSWORD       = os.getenv("ADMIN_PASSWORD")
SECRET_KEY           = os.getenv("SECRET_KEY")
AZURE_TENANT_ID      = os.getenv("AZURE_TENANT_ID")
AZURE_CLIENT_ID      = os.getenv("AZURE_CLIENT_ID")
AZURE_CLIENT_SECRET  = os.getenv("AZURE_CLIENT_SECRET")
AZURE_SUBSCRIPTION_ID = os.getenv("AZURE_SUBSCRIPTION_ID")

# ── Provision EventHub ────────────────────────────────────────────
sender_conn, listener_conn = start_eventhub()

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

AZURE_TENANT_ID={AZURE_TENANT_ID}
AZURE_CLIENT_ID={AZURE_CLIENT_ID}
AZURE_CLIENT_SECRET={AZURE_CLIENT_SECRET}
AZURE_SUBSCRIPTION_ID={AZURE_SUBSCRIPTION_ID}
"""
with open(ENV_FILE, "w") as f:
    f.write(env_contents)
print(".env updated")

# ── Update local .streamlit/secrets.toml ─────────────────────────
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
AZURE_TENANT_ID = "{AZURE_TENANT_ID}"
AZURE_CLIENT_ID = "{AZURE_CLIENT_ID}"
AZURE_CLIENT_SECRET = "{AZURE_CLIENT_SECRET}"
AZURE_SUBSCRIPTION_ID = "{AZURE_SUBSCRIPTION_ID}"
"""
os.makedirs(".streamlit", exist_ok=True)
with open(".streamlit/secrets.toml", "w") as f:
    f.write(secrets_toml)
print(".streamlit/secrets.toml updated")

# ── Update Databricks pipeline config ─────────────────────────────
update_pipeline_connection_string(listener_conn)

# ── Summary ───────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("ALL DONE")
print(f"  EventHub     : rg-uber-events is LIVE")
print(f"  Topic        : {EVENTHUB}")
print(f"  .env         : updated")
print(f"  secrets.toml : updated")
print(f"  Databricks   : pipeline config updated")
print("=" * 60)
print("\n📋 Paste this into Streamlit Cloud → app ⋮ → Settings → Secrets:\n")
print(secrets_toml)
print("Remember to run stop_eventhub.py when done to avoid charges!")