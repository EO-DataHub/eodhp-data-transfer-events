import logging
import sys
import uuid
from collections import defaultdict
from datetime import datetime

from eodhp_utils.pulsar import messages

from billing_scanner.config import Config
from billing_scanner.pulsar_utils import publish_event
from billing_scanner.s3_utils import (
    download_file,
    init_s3_client,
    init_s3_resource,
    list_files,
)
from billing_scanner.state import load_state, save_state
from billing_scanner.subnettree import build_subnet_trees, load_aws_ip_ranges

logger = logging.getLogger(__name__)


class BillingScanner:
    def __init__(self):
        self.config = Config()
        self.s3 = init_s3_resource(self.config.AWS_REGION)
        self.s3_client = init_s3_client(self.config.AWS_REGION)
        self.processed_files = load_state(self.config.STATE_FILE)
        self.publish_event = publish_event

        # Load AWS IP ranges from the URL (which can be overridden via AWS_IP_RANGES_URL env var)
        try:
            ip_data = load_aws_ip_ranges()  # Will use env var AWS_IP_RANGES_URL if set.
            self.current_tree, self.aws_tree = build_subnet_trees(
                ip_data, current_region=self.config.AWS_REGION
            )
        except Exception:
            logger.exception(
                "Failed to load AWS IP ranges; defaulting SKU determination to EGRESS-INTERNET."
            )
            self.current_tree = None
            self.aws_tree = None

    def list_log_files(self) -> list:
        """Return a list of S3 object keys under the configured log folder."""
        return list_files(self.s3_client, self.config.S3_BUCKET, self.config.LOG_FOLDER)

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
            event_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, f"{group_key}")
            billing_event = messages.BillingEvent()
            billing_event.uuid = str(event_uuid)
            billing_event.event_start = group["earliest"].isoformat() + "Z"
            billing_event.event_end = group["latest"].isoformat() + "Z"
            billing_event.sku = group["sku"]
            billing_event.workspace = group["workspace"]
            billing_event.quantity = float(group["data_size"])
            self.publish_event(
                self.config.PULSAR_BROKER_URL, self.config.PULSAR_TOPIC, billing_event
            )
        return True

    def process_log_line(self, line: str, log_filename: str) -> dict:
        """
        Parse a log line and construct an event dictionary.
        Expected field mapping (based on your log header):
          - Index 2: date (e.g., "2025-04-07")
          - Index 3: time (e.g., "13:38:48")
          - Index 5: sc-bytes (data size)
          - Index 8: cs(Host) (domain)
          - Index 9: cs-uri-stem (URI path)
        For notebook access:
          - The workspace is extracted from cs-uri-stem (e.g., "tjellicoe-tpzuk")
        For SKU:
          -  Determine the SKU (pricing category) using the client IP address
            and CIDR lookup via PySubnetTree.
        """
        if line.startswith("#"):
            return None

        fields = line.split()
        try:
            date = fields[2]
            time_str = fields[3]
            sc_bytes = int(fields[5])
            cs_host = fields[8]
            client_ip = fields[6]
            cs_uri_stem = fields[9]

            # Extract the workspace from cs-uri-stem for notebook access.
            # Assume cs-uri-stem is in the form "/notebooks/user/<workspace>/api/...".
            parts = cs_uri_stem.split("/")
            workspace = parts[3] if len(parts) > 3 else "default"

            # Determine the SKU using the client IP and the subnet trees.
            if self.current_tree is not None and self.aws_tree is not None:
                if client_ip in self.current_tree:
                    sku = self.current_tree[client_ip]  # e.g., "EGRESS-REGION"
                elif client_ip in self.aws_tree:
                    sku = self.aws_tree[client_ip]  # e.g., "EGRESS-INTERREGION"
                else:
                    sku = "EGRESS-INTERNET"
            else:
                sku = "EGRESS-INTERNET"

            # Generate the aggregation (or event) key as "<log_filename>-<workspace>-<sku>"
            event_key = f"{log_filename}-{workspace}-{sku}"
            # Generate a UUID based on the event_key using uuid5 for stable deduplication.
            event_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, event_key)

            timestamp_iso = f"{date}T{time_str}Z"

            event = {
                "uuid": str(event_uuid),
                "workspace": workspace,
                "sku": sku,
                "cs-host": cs_host,
                "data_size": sc_bytes,
                "timestamp": timestamp_iso,
                "aggregation_key": event_key,
            }
            return event
        except Exception as e:
            logger.exception(f"Error parsing : {e} ...")
            return None

    def run(self):
        logger.info("Running BillingScanner...")
        all_files = self.list_log_files()
        logger.info(
            f"Found {len(all_files)} file(s) in bucket {self.config.S3_BUCKET} "
            f"under {self.config.LOG_FOLDER}."
        )
        new_files = [key for key in all_files if key not in self.processed_files]
        logger.info(f"Identified {len(new_files)} new file(s) to process.")
        for key in new_files:
            if self.process_log_file(key):
                self.processed_files.add(key)
            else:
                logger.error(f"Failed to process file {key}; it will be retried later.")
        save_state(self.config.STATE_FILE, self.processed_files)
        logger.info("BillingScanner run complete.")


def main():
    scanner = BillingScanner()
    scanner.run()


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    try:
        main()
    except Exception:
        logging.exception("Unexpected error in main:")
        sys.exit(1)
