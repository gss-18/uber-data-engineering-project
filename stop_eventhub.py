import subprocess
import os
import sys
from dotenv import load_dotenv
load_dotenv()

RESOURCE_GROUP = "rg-uber"
NAMESPACE      = "rg-uber-events"
ENV_FILE       = ".env"

DATABRICKS_HOST        = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN       = os.getenv("DATABRICKS_TOKEN")
PIPELINE_ID            = os.getenv("PIPELINE_ID")
DATABRICKS_HTTP_PATH   = os.getenv("DATABRICKS_HTTP_PATH")

def run(cmd):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        exit(1)
    return result.stdout.strip()

# Allow --yes flag to skip confirmation (used by Streamlit)
auto_confirm = "--yes" in sys.argv

if not auto_confirm:
    confirm = input(f"Delete EventHub namespace '{NAMESPACE}'? This stops billing. (yes/no): ")
    if confirm.lower() != "yes":
        print("Cancelled.")
        exit(0)

print(f"Deleting {NAMESPACE}...")
run(f"az eventhubs namespace delete --name {NAMESPACE} --resource-group {RESOURCE_GROUP}")

# Clear EventHub credentials but preserve all other variables
env_contents = f"""CONNECTION_STRING=
EVENT_HUBNAME=
LISTENER_CONNECTION_STRING=

DATABRICKS_HOST={DATABRICKS_HOST}
DATABRICKS_TOKEN={DATABRICKS_TOKEN}
PIPELINE_ID={PIPELINE_ID}
DATABRICKS_HTTP_PATH={DATABRICKS_HTTP_PATH}

ADMIN_USERNAME={os.getenv("ADMIN_USERNAME")}
ADMIN_PASSWORD={os.getenv("ADMIN_PASSWORD")}
SECRET_KEY={os.getenv("SECRET_KEY")}
"""
with open(ENV_FILE, "w") as f:
    f.write(env_contents)

print("\n" + "=" * 60)
print("EventHub DELETED — billing stopped")
print(".env EventHub credentials cleared, Databricks variables preserved")
print("=" * 60)