import logging

import pulsar
from eodhp_utils.pulsar import messages
from eodhp_utils.runner import get_pulsar_client
from pulsar.schema import JsonSchema

logger = logging.getLogger(__name__)


def create_producer(pulsar_topic: str) -> pulsar.Producer:
    """Creates and returns a Pulsar producer for the given topic using the JSON schema for BillingEvent."""
    try:
        client = get_pulsar_client()
        schema = JsonSchema(messages.BillingEvent)
        producer = client.create_producer(pulsar_topic, schema=schema)
        logger.info("Created Pulsar producer for topic: %s", pulsar_topic)
        return producer
    except Exception:
        logger.exception("Error creating producer for topic: %s", pulsar_topic)
        raise
