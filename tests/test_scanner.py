import pytest

from billing_scanner.scanner import BillingScanner
from billing_scanner.subnettree import EgressSKU

# --- Dummy implementations for testing ---


class DummyClassifier:
    """A dummy classifier that always returns REGION for testing."""

    def classify(self, client_ip: str):
        return EgressSKU.REGION


class DummyProducer:
    """A dummy Pulsar producer that collects events."""

    def __init__(self):
        self.sent_events = []

    def send(self, event):
        self.sent_events.append(event)
        return event


class DummyPulsarClient:
    """A dummy Pulsar client whose create_producer returns DummyProducer."""

    def create_producer(self, pulsar_topic, schema):
        return DummyProducer()


# --- Fixture for BillingScanner with dummy overrides ---


@pytest.fixture
def dummy_scanner(monkeypatch, tmp_path):
    """
    Creates a BillingScanner instance with a dummy configuration and overrides
    so that no real network calls occur.
    """
    from billing_scanner.config import Config

    # Create a dummy configuration object.
    dummy_config = Config()
    dummy_config.S3_BUCKET = "dummy-bucket"
    dummy_config.LOG_FOLDER = "dummy-folder/"
    dummy_config.STATE_FILE = str(tmp_path / "processed_logs.json")
    dummy_config.DISTRIBUTION_ID = ""  # No distribution ID for simplicity.
    dummy_config.AWS_REGION = "eu-west-2"
    dummy_config.AWS_IP_RANGES_URL = (
        "https://example.com/dummy.json"  # Dummy URL; fallback will be used.
    )
    dummy_config.PULSAR_TOPIC = "dummy_topic"
    dummy_config.FALLBACK_IP_RANGES_FILE = str(tmp_path / "fallback_ip_ranges.json")
    # Create a dummy fallback file.
    (tmp_path / "fallback_ip_ranges.json").write_text('{"prefixes": []}')

    # Set environment variables.
    monkeypatch.setenv("STATE_FILE", dummy_config.STATE_FILE)
    monkeypatch.setenv("S3_BUCKET", dummy_config.S3_BUCKET)
    monkeypatch.setenv("LOG_FOLDER", dummy_config.LOG_FOLDER)
    monkeypatch.setenv("PULSAR_URL", "pulsar://dummy-pulsar:6650")
    monkeypatch.setenv("PULSAR_TOPIC", dummy_config.PULSAR_TOPIC)
    monkeypatch.setenv("AWS_REGION", dummy_config.AWS_REGION)

    # Override get_pulsar_client and Client to avoid real Pulsar connection.
    monkeypatch.setattr(
        "eodhp_utils.runner.get_pulsar_client", lambda *args, **kwargs: DummyPulsarClient()
    )
    monkeypatch.setattr(
        "eodhp_utils.runner.Client",
        lambda service_url, message_listener_threads=1: DummyPulsarClient(),
    )

    # Instantiate BillingScanner.
    scanner = BillingScanner()
    scanner.config = dummy_config

    # Replace AWSIPClassifier with DummyClassifier.
    scanner.aws_classifier = DummyClassifier()

    # Override S3 file methods.
    scanner.list_log_files = lambda: ["dummy_log.txt"]

    # Define dummy log content with at least 18 tab-separated fields.
    # Fields: 0:timestamp, 1:DistributionId, 2:date, 3:time, 4:x-edge-location,
    # 5:sc-bytes, 6:c-ip, 7:cs-method, 8:cs(Host), 9:cs-uri-stem, 10:sc-status,
    # 11:cs(Referer), 12:cs(User-Agent), 13:cs-uri-query, 14:cs(Cookie),
    # 15:x-edge-result-type, 16:x-edge-request-id, 17:x-host-header.
    dummy_log_content = (
        "#Version: 1.0\n"
        "#Fields:\ttimestamp\tDistributionId\tdate\ttime\tx-edge-location\tsc-bytes\tc-ip\tcs-method\tcs(Host)\tcs-uri-stem\tsc-status\tcs(Referer)\tcs(User-Agent)\tcs-uri-query\tcs(Cookie)\tx-edge-result-type\tx-edge-request-id\tx-host-header\n"
        "1744033128\tEYK26O46YS1D0\t2025-04-07\t13:38:48\tLHR3-C2\t100\t1.1.1.1\tGET\tdummy.example.com\t/notebooks/user/workspace1/api/sessions\t200\t-\t-\t-\t-\tMiss\t-\tworkspace1.eodatahub-workspaces.org.uk\n"
    )
    scanner.download_log_file = lambda key: dummy_log_content

    # Override the producer to a dummy producer.
    dummy_producer = DummyProducer()
    scanner.producer = dummy_producer

    return scanner, dummy_producer


def test_process_log_line_valid(dummy_scanner):
    scanner, _ = dummy_scanner
    # Create a valid log line with 18 tab-separated fields.
    log_line = (
        "1744033128\tdummy\t2025-04-07\t13:38:48\tLHR3-C2\t100\t1.1.1.1\tGET\tdummy.example.com\t"
        "/notebooks/user/workspace1/api/sessions\t200\t-\t-\t-\t-\tMiss\t-\tworkspace1.eodatahub-workspaces.org.uk"
    )
    result = scanner.process_log_line(log_line, "dummy_log.txt")
    assert result is not None, "Expected a valid result but got None."
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
    # For file aggregation, we simulate a file with two valid log lines that share the same aggregation key.
    dummy_log_content = (
        "#Version: 1.0\n"
        "#Fields:\ttimestamp\tDistributionId\tdate\ttime\tx-edge-location\tsc-bytes\tc-ip\tcs-method\tcs(Host)\tcs-uri-stem\tsc-status\tcs(Referer)\tcs(User-Agent)\tcs-uri-query\tcs(Cookie)\tx-edge-result-type\tx-edge-request-id\tx-host-header\n"
        "1744033128\tEYK26O46YS1D0\t2025-04-07\t13:38:48\tLHR3-C2\t398\t1.1.1.1\tGET\tdummy.example.com\t/notebooks/user/workspace1/api/sessions\t200\t-\t-\t-\t-\tMiss\t-\tworkspace1.eodatahub-workspaces.org.uk\n"
        "1744033128\tEYK26O46YS1D0\t2025-04-07\t13:39:00\tLHR3-C2\t200\t1.1.1.1\tGET\tdummy.example.com\t/notebooks/user/workspace1/api/sessions\t200\t-\t-\t-\t-\tMiss\t-\tworkspace1.eodatahub-workspaces.org.uk\n"
    )
    scanner.download_log_file = lambda key: dummy_log_content
    result = scanner.process_log_file("dummy_log.txt")
    assert result is True, "Expected process_log_file to return True."
    # Because both log lines have the same aggregation key ("dummy_log.txt-workspace1-EGRESS-REGION"),
    # they should aggregate into a single event.
    assert (
        len(dummy_producer.sent_events) == 1
    ), f"Expected one aggregated event, got {len(dummy_producer.sent_events)}"
    event = dummy_producer.sent_events[0]
    # Aggregated quantity should equal 398 + 200 = 598.
    assert event.quantity == 598.0
    assert event.event_start == "2025-04-07T13:38:48Z"
    assert event.event_end == "2025-04-07T13:39:00Z"
    assert event.workspace == "workspace1"
    assert event.sku == "EGRESS-REGION"
