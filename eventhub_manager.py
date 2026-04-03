"""
eventhub_manager.py — Shared Azure SDK module
──────────────────────────────────────────────
Used by both local scripts (start_eventhub.py, stop_eventhub.py)
and the Streamlit control panel (control.py).

No print() calls that could leak credentials into the Streamlit UI.
Status updates are passed via an optional callback function.
"""
import os
import requests

try:
    import streamlit as st
    def _get_secret(key: str) -> str:
        try:
            return st.secrets[key]
        except Exception:
            return os.getenv(key)
except Exception:
    def _get_secret(key: str) -> str:
        return os.getenv(key)

from azure.identity import ClientSecretCredential
from azure.mgmt.eventhub import EventHubManagementClient

RESOURCE_GROUP = "rg-uber"
NAMESPACE      = "rg-uber-events"
EVENTHUB       = "ubertopic"
LOCATION       = "eastus"


def _get_mgmt_client() -> EventHubManagementClient:
    credential = ClientSecretCredential(
        tenant_id=_get_secret("AZURE_TENANT_ID"),
        client_id=_get_secret("AZURE_CLIENT_ID"),
        client_secret=_get_secret("AZURE_CLIENT_SECRET")
    )
    return EventHubManagementClient(
        credential=credential,
        subscription_id=_get_secret("AZURE_SUBSCRIPTION_ID")
    )


def start_eventhub(on_status=None) -> tuple[str, str]:
    """
    Provision EventHub namespace, topic, and SAS policies.
    Returns (sender_connection_string, listener_connection_string).

    on_status: optional callable(message: str) for progress updates.
                Use print for local scripts, st.status.update for Streamlit.
    """
    def log(msg):
        if on_status:
            on_status(msg)

    client = _get_mgmt_client()

    log("Creating EventHub namespace...")
    client.namespaces.begin_create_or_update(
        resource_group_name=RESOURCE_GROUP,
        namespace_name=NAMESPACE,
        parameters={
            "location": LOCATION,
            "sku": {"name": "Standard", "tier": "Standard", "capacity": 1}
        }
    ).result()
    log("Namespace ready")

    log("Creating EventHub topic...")
    client.event_hubs.create_or_update(
        resource_group_name=RESOURCE_GROUP,
        namespace_name=NAMESPACE,
        event_hub_name=EVENTHUB,
        parameters={"message_retention_in_days": 1, "partition_count": 2}
    )
    log("Topic ready")

    log("Creating access policies...")
    client.namespaces.create_or_update_authorization_rule(
        resource_group_name=RESOURCE_GROUP,
        namespace_name=NAMESPACE,
        authorization_rule_name="SenderPolicy",
        parameters={"rights": ["Send"]}
    )
    client.namespaces.create_or_update_authorization_rule(
        resource_group_name=RESOURCE_GROUP,
        namespace_name=NAMESPACE,
        authorization_rule_name="ListenerPolicy",
        parameters={"rights": ["Listen"]}
    )

    log("Fetching connection strings...")
    sender_keys   = client.namespaces.list_keys(RESOURCE_GROUP, NAMESPACE, "SenderPolicy")
    listener_keys = client.namespaces.list_keys(RESOURCE_GROUP, NAMESPACE, "ListenerPolicy")

    log("EventHub fully provisioned")
    return (
        sender_keys.primary_connection_string,
        listener_keys.primary_connection_string
    )


def stop_eventhub(on_status=None) -> None:
    """Delete the EventHub namespace — stops billing immediately."""
    def log(msg):
        if on_status:
            on_status(msg)

    client = _get_mgmt_client()
    log("Deleting namespace...")
    client.namespaces.begin_delete(
        resource_group_name=RESOURCE_GROUP,
        namespace_name=NAMESPACE
    ).result()
    log("Namespace deleted — billing stopped")


def update_pipeline_connection_string(listener_conn_str: str, on_status=None) -> None:
    """Update the EventHub connection string in the Databricks DLT pipeline config."""
    def log(msg):
        if on_status:
            on_status(msg)

    host        = _get_secret("DATABRICKS_HOST")
    token       = _get_secret("DATABRICKS_TOKEN")
    pipeline_id = _get_secret("PIPELINE_ID")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    log("Updating Databricks pipeline config...")
    resp = requests.get(
        f"{host}/api/2.0/pipelines/{pipeline_id}",
        headers=headers
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Failed to get pipeline settings: {resp.status_code}")

    pipeline = resp.json()
    config = pipeline.get("spec", {}).get("configuration", {})
    config["connection_string"] = listener_conn_str

    updated_spec = pipeline["spec"]
    updated_spec["configuration"] = config

    resp = requests.put(
        f"{host}/api/2.0/pipelines/{pipeline_id}",
        headers=headers,
        json=updated_spec
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Failed to update pipeline: {resp.status_code}")
    log("Pipeline config updated")


def trigger_pipeline(full_refresh: bool = False):
    """Trigger a Databricks DLT pipeline update. Returns the API response."""
    host        = _get_secret("DATABRICKS_HOST")
    token       = _get_secret("DATABRICKS_TOKEN")
    pipeline_id = _get_secret("PIPELINE_ID")

    return requests.post(
        f"{host}/api/2.0/pipelines/{pipeline_id}/updates",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"full_refresh": full_refresh}
    )