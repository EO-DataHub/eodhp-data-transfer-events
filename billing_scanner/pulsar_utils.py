import logging

from eodhp_utils.pulsar import messages
from eodhp_utils.runner import get_pulsar_client
from pulsar.schema import JsonSchema

logger = logging.getLogger(__name__)


def create_producer(pulsar_topic: str):
    """
    Creates and returns a Pulsar producer for the given topic using the JSON schema for BillingEvent.
    This function relies on get_pulsar_client (from eodhp_utils) to obtain the client
    """
    try:
        client = get_pulsar_client()
        schema = JsonSchema(messages.BillingEvent)
        producer = client.create_producer(pulsar_topic, schema=schema)
        logger.info(f"Created Pulsar producer for topic: {pulsar_topic}")
        return producer
    except Exception:
        logger.exception(f"Error creating producer for topic: {pulsar_topic}")
        raise
