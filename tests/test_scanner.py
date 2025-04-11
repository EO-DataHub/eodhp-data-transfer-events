import pytest

from billing_scanner.scanner import BillingScanner


# A dummy publisher to capture published events.
class DummyPublisher:
    def __init__(self):
        self.events = []

    def publish(self, pulsar_broker, pulsar_topic, event):
        self.events.append(event)
        return True


@pytest.fixture
def dummy_scanner(monkeypatch, tmp_path):
    # Override the STATE_FILE environment variable to a temporary file.
    state_file = tmp_path / "processed_logs.json"
    monkeypatch.setenv("STATE_FILE", str(state_file))
    # Override other environment variables if needed.
    monkeypatch.setenv("S3_BUCKET", "eodh-access-logs-cluster")
    monkeypatch.setenv("LOG_FOLDER", "AWSLogs/312280911266/CloudFront/workspace/")
    monkeypatch.setenv("PULSAR_BROKER_URL", "pulsar://pulsar-proxy:6650")
    monkeypatch.setenv("PULSAR_TOPIC", "billing-events")
    monkeypatch.setenv("AWS_REGION", "eu-west-2")

    # Create a BillingScanner instance.
    scanner = BillingScanner()
    # Override the publish_event method to capture events (dummy publisher).
    dummy_pub = DummyPublisher()  # Assume DummyPublisher is defined in your tests.
    monkeypatch.setattr(
        scanner,
        "publish_event",
        lambda broker, topic, event: dummy_pub.publish(broker, topic, event),
    )
    # Override list_log_files for test control.
    monkeypatch.setattr(scanner, "list_log_files", lambda: ["dummyfile.gz"])
    scanner.processed_files = set()
    scanner._dummy_publisher = dummy_pub
    return scanner


def test_process_log_line(dummy_scanner):
    # Create a sample log line based on the expected header:
    # Fields: 0: timestamp, 1: DistributionId, 2: date, 3: time, 4: x-edge-location,
    # 5: sc-bytes, 6: c-ip, 7: cs-method, 8: cs(Host), 9: cs-uri-stem, ...
    log_line = (
        "1744033128 EYK26O46YS1D0 2025-04-07 13:38:48 LHR3-C2 398 143.58.146.229 "
        "GET dsakofwkmfc6v.cloudfront.net /notebooks/user/tjellicoe-tpzuk/api/sessions 200"
    )

    # Call process_log_line with a dummy filename.
    event_data = dummy_scanner.process_log_line(log_line, "dummyfile.gz")
    assert event_data is not None, "Failed to parse a valid log line"
    # For the aggregation key, our code uses: <log_filename>-<workspace>-<sku>
    # For this log line, cs-uri-stem is "/notebooks/user/tjellicoe-tpzuk/api/sessions"
    # Splitting this path gives: ['', 'notebooks', 'user', 'tjellicoe-tpzuk', 'api', 'sessions']
    # So workspace should be extracted as "tjellicoe-tpzuk".
    expected_group_key = "dummyfile.gz-tjellicoe-tpzuk-EGRESS-INTERNET"
    assert event_data["aggregation_key"] == expected_group_key
    assert event_data["timestamp"] == "2025-04-07T13:38:48Z"
    assert event_data["data_size"] == 398
    assert event_data["workspace"] == "tjellicoe-tpzuk"
    assert event_data["sku"] == "EGRESS-INTERNET"


def test_process_log_file_aggregation(dummy_scanner, monkeypatch):
    # Create two sample log lines that should aggregate together.
    line1 = (
        "1744033128 EYK26O46YS1D0 2025-04-07 13:38:48 LHR3-C2 100 143.58.146.229 "
        "GET dsakofwkmfc6v.cloudfront.net /notebooks/user/tjellicoe-tpzuk/api/sessions 200"
    )
    line2 = (
        "1744033128 EYK26O46YS1D0 2025-04-07 13:39:48 LHR3-C2 150 143.58.146.229 "
        "GET dsakofwkmfc6v.cloudfront.net /notebooks/user/tjellicoe-tpzuk/api/sessions 200"
    )
    dummy_content = "\n".join(
        [
            "#Version: 1.0",
            "#Fields: timestamp DistributionId date time x-edge-location sc-bytes c-ip "
            "cs-method cs(Host) cs-uri-stem ...",
            line1,
            line2,
        ]
    )
    # Override download_log_file to return dummy_content.
    monkeypatch.setattr(dummy_scanner, "download_log_file", lambda key: dummy_content)
    # Clear processed_files.
    dummy_scanner.processed_files = set()
    # Run the scanner.
    dummy_scanner.run()
    # Retrieve events published via our dummy publisher.
    published_events = dummy_scanner._dummy_publisher.events
    # Both log lines share the same grouping key, so expect one aggregated event.
    assert len(published_events) == 1, "Expected one aggregated event, got more."
    aggregated_event = published_events[0]
    # Check that the total quantity is the sum of both lines (100 + 150 = 250).
    assert aggregated_event.quantity == 250.0, "Aggregated quantity incorrect."
    # Earliest and latest timestamps should reflect line1 and line2 respectively.
    assert (
        aggregated_event.event_start == "2025-04-07T13:38:48Z"
    ), "Event start timestamp incorrect."
    assert aggregated_event.event_end == "2025-04-07T13:39:48Z", "Event end timestamp incorrect."
