"""
eventhub_manager.py — Shared Azure SDK module
──────────────────────────────────────────────
Used by both local scripts (start_eventhub.py, stop_eventhub.py)
and the Streamlit control panel (control.py).

Replaces all az CLI subprocess calls with Azure Python SDK calls.
Works on Streamlit Cloud since it installs via pip, no system binaries needed.
"""
import os
import time
import requests

try:
    import streamlit as st
    def _get_secret(key: str) -> str:
        try:
            return st.secrets[key]
        except Exception:
            return os.getenv(key)
except Exception:
    # Running outside Streamlit (local script) — use os.getenv only
    def _get_secret(key: str) -> str:
        return os.getenv(key)

from azure.identity import ClientSecretCredential
from azure.mgmt.eventhub import EventHubManagementClient

RESOURCE_GROUP = "rg-uber"
NAMESPACE      = "rg-uber-events"
EVENTHUB       = "ubertopic"
LOCATION       = "eastus"


def _get_mgmt_client() -> EventHubManagementClient:
    """Build an authenticated Azure EventHub management client."""
    credential = ClientSecretCredential(
        tenant_id=_get_secret("AZURE_TENANT_ID"),
        client_id=_get_secret("AZURE_CLIENT_ID"),
        client_secret=_get_secret("AZURE_CLIENT_SECRET")
    )
    return EventHubManagementClient(
        credential=credential,
        subscription_id=_get_secret("AZURE_SUBSCRIPTION_ID")
    )


def start_eventhub() -> tuple[str, str]:
    """
    Provision EventHub namespace, topic, and SAS policies.
    Returns (sender_connection_string, listener_connection_string).
    """
    client = _get_mgmt_client()

    # 1. Create namespace (Standard tier)
    print("Creating EventHub namespace...")
    client.namespaces.begin_create_or_update(
        resource_group_name=RESOURCE_GROUP,
        namespace_name=NAMESPACE,
        parameters={
            "location": LOCATION,
            "sku": {"name": "Standard", "tier": "Standard", "capacity": 1}
        }
    ).result()  # .result() blocks until provisioning completes
    print(f"  Namespace '{NAMESPACE}' ready")

    # 2. Create EventHub topic
    print("Creating EventHub topic...")
    client.event_hubs.create_or_update(
        resource_group_name=RESOURCE_GROUP,
        namespace_name=NAMESPACE,
        event_hub_name=EVENTHUB,
        parameters={"message_retention_in_days": 1, "partition_count": 2}
    )
    print(f"  Topic '{EVENTHUB}' ready")

    # 3. Create SenderPolicy (Send rights)
    print("Creating SenderPolicy...")
    client.namespaces.create_or_update_authorization_rule(
        resource_group_name=RESOURCE_GROUP,
        namespace_name=NAMESPACE,
        authorization_rule_name="SenderPolicy",
        parameters={"rights": ["Send"]}
    )

    # 4. Create ListenerPolicy (Listen rights)
    print("Creating ListenerPolicy...")
    client.namespaces.create_or_update_authorization_rule(
        resource_group_name=RESOURCE_GROUP,
        namespace_name=NAMESPACE,
        authorization_rule_name="ListenerPolicy",
        parameters={"rights": ["Listen"]}
    )

    # 5. Fetch connection strings
    print("Fetching connection strings...")
    sender_keys   = client.namespaces.list_keys(RESOURCE_GROUP, NAMESPACE, "SenderPolicy")
    listener_keys = client.namespaces.list_keys(RESOURCE_GROUP, NAMESPACE, "ListenerPolicy")

    sender_conn   = sender_keys.primary_connection_string
    listener_conn = listener_keys.primary_connection_string

    print("EventHub fully provisioned")
    return sender_conn, listener_conn


def stop_eventhub() -> None:
    """Delete the EventHub namespace — stops billing immediately."""
    client = _get_mgmt_client()
    print(f"Deleting namespace '{NAMESPACE}'...")
    client.namespaces.begin_delete(
        resource_group_name=RESOURCE_GROUP,
        namespace_name=NAMESPACE
    ).result()
    print("Namespace deleted — billing stopped")


def update_pipeline_connection_string(listener_conn_str: str) -> None:
    """Update the EventHub connection string in the Databricks DLT pipeline config."""
    host       = _get_secret("DATABRICKS_HOST")
    token      = _get_secret("DATABRICKS_TOKEN")
    pipeline_id = _get_secret("PIPELINE_ID")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    print("Fetching current Databricks pipeline settings...")
    resp = requests.get(
        f"{host}/api/2.0/pipelines/{pipeline_id}",
        headers=headers
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Failed to get pipeline settings: {resp.text}")

    pipeline = resp.json()
    config = pipeline.get("spec", {}).get("configuration", {})
    config["connection_string"] = listener_conn_str

    updated_spec = pipeline["spec"]
    updated_spec["configuration"] = config

    print("Updating connection_string in Databricks pipeline...")
    resp = requests.put(
        f"{host}/api/2.0/pipelines/{pipeline_id}",
        headers=headers,
        json=updated_spec
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Failed to update pipeline: {resp.text}")
    print("Pipeline configuration updated successfully")


def trigger_pipeline(full_refresh: bool = False) -> dict:
    """Trigger a Databricks DLT pipeline update. Returns the API response."""
    host        = _get_secret("DATABRICKS_HOST")
    token       = _get_secret("DATABRICKS_TOKEN")
    pipeline_id = _get_secret("PIPELINE_ID")

    resp = requests.post(
        f"{host}/api/2.0/pipelines/{pipeline_id}/updates",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"full_refresh": full_refresh}
    )
    return resp