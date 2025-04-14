import logging

from eodhp_utils.pulsar import messages
from eodhp_utils.runner import get_pulsar_client
from pulsar.schema import JsonSchema

logger = logging.getLogger(__name__)

_producer_cache = {}


def publish_event(pulsar_broker: str, pulsar_topic: str, event) -> bool:
    """
    Publish a BillingEvent to Pulsar.

    """
    try:
        client = get_pulsar_client(pulsar_url=pulsar_broker)
        if pulsar_topic in _producer_cache:
            producer = _producer_cache[pulsar_topic]
        else:
            schema = JsonSchema(messages.BillingEvent)
            producer = client.create_producer(pulsar_topic, schema=schema)
            _producer_cache[pulsar_topic] = producer
        producer.send(event)
        logger.info(f"Published event {event.uuid} via eodhp_utils.")
        return True
    except Exception:
        logger.exception("Error publishing event via eodhp_utils")
        return False
