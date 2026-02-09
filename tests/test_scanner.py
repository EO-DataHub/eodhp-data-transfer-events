from pathlib import Path

import pytest
from eodhp_utils.pulsar.messages import BillingEvent

from billing_scanner.config import Config
from billing_scanner.scanner import BillingScanner
from billing_scanner.subnettree import EgressSKU


class DummyClassifier:
    """A dummy classifier that always returns REGION for testing."""

    def classify(self, client_ip: str) -> EgressSKU:
        return EgressSKU.REGION


class DummyProducer:
    """A dummy Pulsar producer that collects events."""

    def __init__(self) -> None:
        self.sent_events: list[BillingEvent] = []

    def send(self, event: BillingEvent) -> BillingEvent:
        self.sent_events.append(event)
        return event


class DummyPulsarClient:
    """A dummy Pulsar client whose create_producer returns DummyProducer."""

    def create_producer(self, pulsar_topic: str, schema: object) -> DummyProducer:
        return DummyProducer()


@pytest.fixture
def dummy_scanner(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[BillingScanner, DummyProducer]:
    """Creates a BillingScanner instance with a dummy configuration and overrides
    so that no real network calls occur.
    """
    dummy_config = Config()
    dummy_config.S3_BUCKET = "dummy-bucket"
    dummy_config.LOG_FOLDER = "dummy-folder/"
    dummy_config.STATE_FILE = str(tmp_path / "processed_logs.json")
    dummy_config.DISTRIBUTION_ID = ""
    dummy_config.AWS_REGION = "eu-west-2"
    dummy_config.AWS_IP_RANGES_URL = "https://example.com/dummy.json"
    dummy_config.PULSAR_TOPIC = "dummy_topic"
    dummy_config.FALLBACK_IP_RANGES_FILE = str(tmp_path / "fallback_ip_ranges.json")
    (tmp_path / "fallback_ip_ranges.json").write_text('{"prefixes": []}')

    monkeypatch.setenv("STATE_FILE", dummy_config.STATE_FILE)
    monkeypatch.setenv("S3_BUCKET", dummy_config.S3_BUCKET)
    monkeypatch.setenv("LOG_FOLDER", dummy_config.LOG_FOLDER)
    monkeypatch.setenv("PULSAR_URL", "pulsar://dummy-pulsar:6650")
    monkeypatch.setenv("PULSAR_TOPIC", dummy_config.PULSAR_TOPIC)
    monkeypatch.setenv("AWS_REGION", dummy_config.AWS_REGION)

    monkeypatch.setattr("eodhp_utils.runner.get_pulsar_client", lambda *args, **kwargs: DummyPulsarClient())
    monkeypatch.setattr(
        "eodhp_utils.runner.Client",
        lambda service_url, message_listener_threads=1: DummyPulsarClient(),
    )

    scanner = BillingScanner()
    scanner.config = dummy_config

    dummy_classifier = DummyClassifier()
    object.__setattr__(scanner, "aws_classifier", dummy_classifier)

    dummy_log_content = (
        "#Version: 1.0\n"
        "#Fields:\tdate\ttime\tx-edge-location\tsc-bytes\tc-ip\tcs-method\tcs(Host)\tcs-uri-stem\tsc-status\tcs(Referer)\tcs(User-Agent)\tcs-uri-query\tcs(Cookie)\tx-edge-result-type\tx-edge-request-id\tx-host-header\n"
        "2025-04-07\t13:38:48\tLHR3-C2\t100\t1.1.1.1\tGET\tdummy.example.com\t/notebooks/user/workspace1/api/sessions\t200\t-\t-\t-\t-\tMiss\t-\tworkspace1.eodatahub-workspaces.org.uk\n"
    )
    monkeypatch.setattr(scanner, "download_log_file", lambda key: dummy_log_content)

    dummy_producer = DummyProducer()
    scanner.producer = dummy_producer

    return scanner, dummy_producer


def test_process_log_line_valid(dummy_scanner: tuple[BillingScanner, DummyProducer]) -> None:
    scanner, _ = dummy_scanner
    log_line = (
        "2025-04-07\t13:38:48\tLHR3-C2\t100\t1.1.1.1\tGET\tdummy.example.com\t"
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


def test_process_log_line_comment(dummy_scanner: tuple[BillingScanner, DummyProducer]) -> None:
    scanner, _ = dummy_scanner
    comment_line = "# This is a comment"
    result = scanner.process_log_line(comment_line, "dummy_log.txt")
    assert result is None


def test_process_log_file_aggregation(
    dummy_scanner: tuple[BillingScanner, DummyProducer], monkeypatch: pytest.MonkeyPatch
) -> None:
    scanner, dummy_producer = dummy_scanner
    dummy_log_content = (
        "#Version: 1.0\n"
        "#Fields:\tdate\ttime\tx-edge-location\tsc-bytes\tc-ip\tcs-method\tcs(Host)\tcs-uri-stem\tsc-status\tcs(Referer)\tcs(User-Agent)\tcs-uri-query\tcs(Cookie)\tx-edge-result-type\tx-edge-request-id\tx-host-header\n"
        "2025-04-07\t13:38:48\tLHR3-C2\t398\t1.1.1.1\tGET\tdummy.example.com\t/notebooks/user/workspace1/api/sessions\t200\t-\t-\t-\t-\tMiss\t-\tworkspace1.eodatahub-workspaces.org.uk\n"
        "2025-04-07\t13:39:00\tLHR3-C2\t200\t1.1.1.1\tGET\tdummy.example.com\t/notebooks/user/workspace1/api/sessions\t200\t-\t-\t-\t-\tMiss\t-\tworkspace1.eodatahub-workspaces.org.uk\n"
    )
    monkeypatch.setattr(scanner, "download_log_file", lambda key: dummy_log_content)
    result = scanner.process_log_file("dummy_log.txt")
    assert result is True, "Expected process_log_file to return True."
    assert len(dummy_producer.sent_events) == 1, (
        f"Expected one aggregated event, got {len(dummy_producer.sent_events)}"
    )
    event = dummy_producer.sent_events[0]
    assert event.quantity == 598.0
    assert event.event_start == "2025-04-07T13:38:48Z"
    assert event.event_end == "2025-04-07T13:39:00Z"
    assert event.workspace == "workspace1"
    assert event.sku == "EGRESS-REGION"
