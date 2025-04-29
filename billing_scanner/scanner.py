import logging
import uuid
from collections import defaultdict
from datetime import datetime

from eodhp_utils.pulsar import messages

from billing_scanner.config import Config
from billing_scanner.pulsar_utils import create_producer
from billing_scanner.s3_utils import (
    download_file,
    init_s3_client,
    init_s3_resource,
    list_files,
)
from billing_scanner.state import ScannerState
from billing_scanner.subnettree import AWSIPClassifier

logger = logging.getLogger(__name__)


class BillingScanner:
    def __init__(self):
        self.config = Config()
        self.s3 = init_s3_resource(self.config.AWS_REGION)
        self.s3_client = init_s3_client(self.config.AWS_REGION)
        self.producer = create_producer(self.config.PULSAR_TOPIC)
        # Load AWS IP ranges from the URL
        try:
            self.aws_classifier = AWSIPClassifier(
                self.config.AWS_IP_RANGES_URL,
                self.config.AWS_REGION,
                fallback_file=self.config.FALLBACK_IP_RANGES_FILE,
            )
        except Exception:
            logger.exception("Cannot load AWS IP ranges; aborting to prevent overcharging.")
            raise

    def download_log_file(self, key: str) -> str:
        """Download the content of an S3 log file; decompress if necessary."""
        return download_file(self.s3_client, self.config.S3_BUCKET, key)

    def process_log_file(self, key: str) -> bool:
        """Process a single log file from S3:
        - Download its content.
        - Process each log line to aggregate billing data.
        - Publish one BillingEvent per aggregated group.
        """
        logger.info(f"Processing log file: {key}")
        content = self.download_log_file(key)
        if not content:
            return False

        # Aggregate log lines by a composite key: (log filename, workspace, SKU)
        aggregation = defaultdict(
            lambda: {
                "data_size": 0,
                "earliest": None,
                "latest": None,
                "workspace": None,
                "sku": None,
            }
        )
        for line in content.splitlines():
            event_data = self.process_log_line(line, key)
            if event_data:
                group_key = event_data["aggregation_key"]
                group = aggregation[group_key]
                group["data_size"] += event_data["data_size"]
                ts = datetime.fromisoformat(event_data["timestamp"].replace("Z", ""))
                if group["earliest"] is None or ts < group["earliest"]:
                    group["earliest"] = ts
                if group["latest"] is None or ts > group["latest"]:
                    group["latest"] = ts
                group["workspace"] = event_data["workspace"]
                group["sku"] = event_data["sku"]

        # For each aggregated group, generate one BillingEvent.
        for group_key, group in aggregation.items():
            # Generate a unique and repeatable UUID using uuid.uuid5 from the composite key.
            event_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, group_key)
            billing_event = messages.BillingEvent()
            billing_event.uuid = str(event_uuid)
            billing_event.event_start = group["earliest"].isoformat() + "Z"
            billing_event.event_end = group["latest"].isoformat() + "Z"
            billing_event.sku = group["sku"]
            billing_event.workspace = group["workspace"]
            billing_event.quantity = float(group["data_size"])
            self.producer.send(billing_event)
        return True

    def process_log_line(self, line: str, log_filename: str) -> dict:
        """
        Parse a log line and construct an event dictionary.
        Expected field mapping (based on your log header):
          - Index 2: date (e.g., "2025-04-07")
          - Index 3: time (e.g., "13:38:48")
          - Index 5: sc-bytes (data size)
          - Field index 6: c-ip (client IP)
        - Field index 17: x-host-header
        use the x-host-header to extract the workspace. It is expected to have the form:
        {workspace}[.{cluster}].eodatahub-workspaces.org.uk
        If x-host-header does not match that pattern, the log line is skipped.
        For SKU:
          -  Determine the SKU (pricing category) using the client IP address
            and CIDR lookup via PySubnetTree.
        """
        if not line.strip() or line.startswith("#"):
            return None

        # Split by tab
        fields = line.split("\t")
        date = fields[2]
        time_str = fields[3]
        sc_bytes = int(fields[5])
        client_ip = fields[6]

        # x‑host‑header field
        x_host_header = fields[17] if len(fields) > 17 else ""

        suffix = f".{self.config.WORKSPACES_DOMAIN}"
        if not x_host_header.endswith(suffix):
            logger.info("Host '%s' doesn't end in '%s'; ignoring line", x_host_header, suffix)
            return None

        # strip off the suffix, then take the first label as the workspace
        host_prefix = x_host_header[: -len(suffix)]
        workspace = host_prefix.split(".", 1)[0]

        # If workspace cannot be determined, ignore this log line.
        if not workspace:
            logger.info(
                "Unable to determine workspace from host header '%s'; ignoring line.",
                x_host_header,
            )
            return None

        # Determine the SKU using the client IP and the subnet trees.
        sku_enum = self.aws_classifier.classify(client_ip)
        sku = sku_enum.value

        # Generate the aggregation (or event) key as "<log_filename>-<workspace>-<sku>"
        aggregation_key = f"{log_filename}-{workspace}-{sku}"
        # Generate a UUID based on the event_key using uuid5 for stable deduplication.
        event_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, aggregation_key)

        timestamp_iso = f"{date}T{time_str}Z"

        event = {
            "uuid": str(event_uuid),
            "workspace": workspace,
            "sku": sku,
            "data_size": sc_bytes,
            "timestamp": timestamp_iso,
            "aggregation_key": aggregation_key,
        }
        return event

    def run(self):
        logger.info("Running BillingScanner...")
        # Build the S3 prefix
        prefix = self.config.LOG_FOLDER
        if self.config.DISTRIBUTION_ID:
            prefix = f"{prefix}{self.config.DISTRIBUTION_ID}."
        with ScannerState(self.config.STATE_FILE) as state:
            start_after = state.last_processed or ""
            logger.info(
                f"Listing from bucket={self.config.S3_BUCKET} prefix={prefix} start_after={start_after!r}"
            )
            # List all keys after the last-processed marker
            all_keys = list_files(
                self.s3_client,
                self.config.S3_BUCKET,
                prefix,
                start_after=start_after,
            )
            logger.info(
                f"Found {len(all_keys)} file(s) in bucket {self.config.S3_BUCKET} "
                f"under {self.config.LOG_FOLDER}."
            )

            # Only process those strictly after the last_processed marker
            new_keys = [k for k in all_keys if k > start_after]
            logger.info(f"Identified {len(new_keys)} new file(s) to process.")
            for key in new_keys:
                try:
                    success = self.process_log_file(key)
                except Exception as err:
                    logger.error(f"Error processing '{key}': {err}")
                    # fail fast so we retry this key next time
                    raise
                if not success:
                    # if we returned False (e.g. empty file), retry next run
                    logger.warning(f"No content in '{key}', will retry later.")
                    break

                # only mark as done once we know it succeeded
                state.mark_last_processed(key)

        logger.info("BillingScanner run complete.")
