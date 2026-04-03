"""
stop_eventhub.py — Local helper script
──────────────────────────────────────
Deletes EventHub namespace and clears connection strings from Key Vault.
The Streamlit app will automatically show EventHub as OFFLINE since
Key Vault returns empty strings — no manual secrets paste needed.
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")

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

# ── Delete EventHub + clear Key Vault ────────────────────────────
print("Stopping EventHub...")
stop_eventhub(on_status=print)
print("Key Vault secrets cleared ✅")

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

print("\n" + "=" * 60)
print("EventHub DELETED — billing stopped")
print("Key Vault cleared — app will show EventHub as OFFLINE ✅")
print(".env and secrets.toml cleared")
print("=" * 60)