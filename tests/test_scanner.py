import pytest

from billing_scanner.scanner import BillingScanner
from billing_scanner.subnettree import EgressSKU


class DummyClassifier:
    """A dummy classifier returning fixed SKU values based on client IP."""

    def classify(self, client_ip: str):
        # For testing aggregation, we'll assume all IPs are REGION.
        return EgressSKU.REGION


class DummyProducer:
    """A dummy Pulsar producer that collects events."""

    def __init__(self):
        self.sent_events = []

    def send(self, event):
        self.sent_events.append(event)
        return event


class DummyPulsarClient:
    """A dummy Pulsar client that returns a DummyProducer."""

    def create_producer(self, pulsar_topic, schema):
        return DummyProducer()


# --- Fixture for BillingScanner with dummy overrides ---


@pytest.fixture
def dummy_scanner(monkeypatch, tmp_path):
    """
    Creates a BillingScanner instance with a dummy configuration and overrides
    for external dependencies so that no real network calls occur.
    """
    from billing_scanner.config import Config

    # Create a dummy configuration object.
    dummy_config = Config()
    dummy_config.S3_BUCKET = "dummy-bucket"
    dummy_config.LOG_FOLDER = "dummy-folder/"
    dummy_config.STATE_FILE = str(tmp_path / "processed_logs.json")
    dummy_config.DISTRIBUTION_ID = ""
    dummy_config.AWS_REGION = "eu-west-2"
    dummy_config.AWS_IP_RANGES_URL = (
        "https://example.com/dummy.json"  # Dummy URL; fallback used if needed.
    )
    dummy_config.PULSAR_TOPIC = "dummy_topic"
    dummy_config.FALLBACK_IP_RANGES_FILE = str(tmp_path / "fallback_ip_ranges.json")
    # Write a dummy fallback file so that fallback logic is available.
    (tmp_path / "fallback_ip_ranges.json").write_text('{"prefixes": []}')

    # Set environment variables.
    monkeypatch.setenv("STATE_FILE", dummy_config.STATE_FILE)
    monkeypatch.setenv("S3_BUCKET", dummy_config.S3_BUCKET)
    monkeypatch.setenv("LOG_FOLDER", dummy_config.LOG_FOLDER)
    monkeypatch.setenv("PULSAR_URL", "pulsar://dummy-pulsar:6650")
    monkeypatch.setenv("PULSAR_TOPIC", dummy_config.PULSAR_TOPIC)
    monkeypatch.setenv("AWS_REGION", dummy_config.AWS_REGION)

    # Override get_pulsar_client and Client so that no real Pulsar connection is attempted.
    monkeypatch.setattr(
        "eodhp_utils.runner.get_pulsar_client", lambda *args, **kwargs: DummyPulsarClient()
    )
    monkeypatch.setattr(
        "eodhp_utils.runner.Client",
        lambda service_url, message_listener_threads=1: DummyPulsarClient(),
    )

    # Instantiate BillingScanner; it will use the patched Pulsar client.
    scanner = BillingScanner()
    scanner.config = dummy_config

    # Replace the AWSIPClassifier with our DummyClassifier that always returns REGION.
    scanner.aws_classifier = DummyClassifier()

    # Override S3 file methods to avoid external calls.
    scanner.list_log_files = lambda: ["dummy_log.txt"]

    # Define dummy log content as a tab-separated string.
    # Change both log records to have the same client IP "1.1.1.1" so that they aggregate.
    dummy_log_content = (
        "#Version: 1.0\n"
        "#Fields:\ttimestamp\tDistributionId\tdate\ttime\tx-edge-location\tsc-bytes\tc-ip\tcs-method\tcs(Host)\tcs-uri-stem\t...\n"
        "1744033128\tEYK26O46YS1D0\t2025-04-07\t13:38:48\tLHR3-C2\t398\t1.1.1.1\tGET\tdsakofwkmfc6v.cloudfront.net\t/notebooks/user/workspace1/api/sessions\t200\t...\n"
        "1744033128\tEYK26O46YS1D0\t2025-04-07\t13:39:00\tLHR3-C2\t200\t1.1.1.1\tGET\tdsakofwkmfc6v.cloudfront.net\t/notebooks/user/workspace1/api/sessions\t200\t...\n"
    )
    scanner.download_log_file = lambda key: dummy_log_content

    # Override the producer to a dummy producer.
    dummy_producer = DummyProducer()
    scanner.producer = dummy_producer

    return scanner, dummy_producer


def test_process_log_line_valid(dummy_scanner):
    scanner, _ = dummy_scanner
    log_line = (
        "1744033128\tdummy\t2025-04-07\t13:38:48\tLHR3-C2\t100\t1.1.1.1\tGET\t"
        "dummy.example.com\t/notebooks/user/workspace1/api/sessions"
    )
    result = scanner.process_log_line(log_line, "dummy_log.txt")
    assert result is not None
    expected_key = "dummy_log.txt-workspace1-EGRESS-REGION"
    assert result["aggregation_key"] == expected_key
    assert result["workspace"] == "workspace1"
    assert result["sku"] == "EGRESS-REGION"
    assert result["data_size"] == 100
    assert result["timestamp"] == "2025-04-07T13:38:48Z"


def test_process_log_line_comment(dummy_scanner):
    scanner, _ = dummy_scanner
    comment_line = "# This is a comment"
    result = scanner.process_log_line(comment_line, "dummy_log.txt")
    assert result is None


def test_process_log_file_aggregation(dummy_scanner):
    scanner, dummy_producer = dummy_scanner
    result = scanner.process_log_file("dummy_log.txt")
    assert result is True
    # Both log lines in our dummy log have the same aggregation key
    # ("dummy_log.txt-workspace1-EGRESS-REGION"),
    # so they should aggregate into one event.
    assert len(dummy_producer.sent_events) == 1
    event = dummy_producer.sent_events[0]
    # The aggregated quantity should equal 398 + 200 = 598.
    assert event.quantity == 598.0
    assert event.event_start == "2025-04-07T13:38:48Z"
    assert event.event_end == "2025-04-07T13:39:00Z"
    assert event.workspace == "workspace1"
    assert event.sku == "EGRESS-REGION"
