"""
stop_eventhub.py — Local helper script
──────────────────────────────────────
Run this locally to delete EventHub and stop billing.
On Streamlit Cloud, the same logic runs via the control panel button.
"""
import os
import sys
from dotenv import load_dotenv
load_dotenv()

from eventhub_manager import stop_eventhub

EVENTHUB              = "ubertopic"
ENV_FILE              = ".env"
DATABRICKS_HOST       = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN      = os.getenv("DATABRICKS_TOKEN")
PIPELINE_ID           = os.getenv("PIPELINE_ID")
DATABRICKS_HTTP_PATH  = os.getenv("DATABRICKS_HTTP_PATH")
ADMIN_USERNAME        = os.getenv("ADMIN_USERNAME")
ADMIN_PASSWORD        = os.getenv("ADMIN_PASSWORD")
SECRET_KEY            = os.getenv("SECRET_KEY")
AZURE_TENANT_ID       = os.getenv("AZURE_TENANT_ID")
AZURE_CLIENT_ID       = os.getenv("AZURE_CLIENT_ID")
AZURE_CLIENT_SECRET   = os.getenv("AZURE_CLIENT_SECRET")
AZURE_SUBSCRIPTION_ID = os.getenv("AZURE_SUBSCRIPTION_ID")

# ── Confirmation ──────────────────────────────────────────────────
auto_confirm = "--yes" in sys.argv
if not auto_confirm:
    confirm = input("Delete EventHub namespace 'rg-uber-events'? This stops billing. (yes/no): ")
    if confirm.lower() != "yes":
        print("Cancelled.")
        exit(0)

# ── Delete EventHub ───────────────────────────────────────────────
stop_eventhub()

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

AZURE_TENANT_ID={AZURE_TENANT_ID}
AZURE_CLIENT_ID={AZURE_CLIENT_ID}
AZURE_CLIENT_SECRET={AZURE_CLIENT_SECRET}
AZURE_SUBSCRIPTION_ID={AZURE_SUBSCRIPTION_ID}
"""
with open(ENV_FILE, "w") as f:
    f.write(env_contents)
print(".env EventHub credentials cleared")

# ── Clear local .streamlit/secrets.toml ──────────────────────────
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
AZURE_TENANT_ID = "{AZURE_TENANT_ID}"
AZURE_CLIENT_ID = "{AZURE_CLIENT_ID}"
AZURE_CLIENT_SECRET = "{AZURE_CLIENT_SECRET}"
AZURE_SUBSCRIPTION_ID = "{AZURE_SUBSCRIPTION_ID}"
"""
os.makedirs(".streamlit", exist_ok=True)
with open(".streamlit/secrets.toml", "w") as f:
    f.write(secrets_toml)
print(".streamlit/secrets.toml cleared")

# ── Summary ───────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("EventHub DELETED — billing stopped")
print(".env and secrets.toml cleared")
print("=" * 60)
print("\n📋 Paste this into Streamlit Cloud → app ⋮ → Settings → Secrets")
print("   to show EventHub as OFFLINE in the deployed app:\n")
print(secrets_toml)