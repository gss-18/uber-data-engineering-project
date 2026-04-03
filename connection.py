import json
import os
from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv
load_dotenv()

from data import generate_uber_ride_confirmation
from eventhub_manager import get_connection_strings

# Module-level producer — reused across calls to avoid paying AMQP
# connection setup (TLS + SASL handshake) on every booking.
_producer: EventHubProducerClient | None = None
_producer_conn_str: str | None = None


def _get_producer() -> EventHubProducerClient | None:
    global _producer, _producer_conn_str

    # Always fetch from Key Vault so we get the latest connection string
    # even after EventHub has been recreated in the same session.
    conn_str, _ = get_connection_strings()
    hub_name    = "ubertopic"

    if not conn_str:
        return None

    # Re-create producer only when the connection string changes.
    if _producer is None or conn_str != _producer_conn_str:
        if _producer is not None:
            try:
                _producer.close()
            except Exception:
                pass
        _producer = EventHubProducerClient.from_connection_string(
            conn_str, eventhub_name=hub_name
        )
        _producer_conn_str = conn_str

    return _producer


def send_to_event_hub(ride_data=None, batch_size=1):
    global _producer
    try:
        producer = _get_producer()
        if producer is None:
            return False

        ride_json   = json.dumps(ride_data)
        event_batch = producer.create_batch()
        event_batch.add(EventData(ride_json))
        producer.send_batch(event_batch)
        return "Successfully sent to Event Hub"

    except Exception as e:
        # Force reconnect on the next call
        _producer = None
        return False


if __name__ == "__main__":
    print("=" * 80)
    print("SINGLE RIDE CONFIRMATION")
    print("=" * 80)
    ride = generate_uber_ride_confirmation()
    print(json.dumps(ride, indent=2))

    print("\n" + "=" * 80)
    print("SENDING SINGLE RIDE TO EVENT HUB")
    result = send_to_event_hub(ride)
    print(f"Single ride sent to Event Hub: {result}")