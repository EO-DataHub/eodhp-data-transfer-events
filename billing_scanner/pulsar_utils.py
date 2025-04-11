import logging

from eodhp_utils.pulsar import messages
from eodhp_utils.runner import get_pulsar_client
from pulsar.schema import JsonSchema

logger = logging.getLogger(__name__)


def publish_event(pulsar_broker: str, pulsar_topic: str, event) -> bool:
    """
    Publish a BillingEvent to Pulsar.

    """
    try:
        client = get_pulsar_client(pulsar_url=pulsar_broker)
        schema = JsonSchema(messages.BillingEvent)
        producer = client.create_producer(pulsar_topic, schema=schema)
        producer.send(event)
        producer.close()
        logger.info(f"Published event {event.uuid} via eodhp_utils.")
        return True
    except Exception:
        logger.exception("Error publishing event via eodhp_utils")
        return False
