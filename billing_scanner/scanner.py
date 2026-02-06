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
    def __init__(self) -> None:
        self.config = Config()
        self.s3 = init_s3_resource(self.config.AWS_REGION)
        self.s3_client = init_s3_client(self.config.AWS_REGION)
        self.producer = create_producer(self.config.PULSAR_TOPIC)
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
        """Process a single log file from S3."""
        logger.info("Processing log file: %s", key)
        content = self.download_log_file(key)
        if not content:
            return False

        aggregation: dict[str, dict[str, int | str | datetime | None]] = defaultdict(
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
                ts = datetime.fromisoformat(str(event_data["timestamp"]).replace("Z", ""))
                if group["earliest"] is None or ts < group["earliest"]:
                    group["earliest"] = ts
                if group["latest"] is None or ts > group["latest"]:
                    group["latest"] = ts
                group["workspace"] = event_data["workspace"]
                group["sku"] = event_data["sku"]

        for group_key, group in aggregation.items():
            event_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, group_key)
            billing_event = messages.BillingEvent()
            billing_event.uuid = str(event_uuid)
            earliest = group["earliest"]
            latest = group["latest"]
            if isinstance(earliest, datetime) and isinstance(latest, datetime):
                billing_event.event_start = earliest.isoformat() + "Z"
                billing_event.event_end = latest.isoformat() + "Z"
            billing_event.sku = str(group["sku"])
            billing_event.workspace = str(group["workspace"])
            billing_event.quantity = float(group["data_size"])
            self.producer.send(billing_event)
        return True

    def process_log_line(self, line: str, log_filename: str) -> dict[str, str | int] | None:
        """Parse a log line and construct an event dictionary."""
        if not line.strip() or line.startswith("#"):
            return None

        fields = line.split("\t")
        date = fields[0]
        time_str = fields[1]
        sc_bytes = int(fields[3])
        client_ip = fields[4]

        # x-host-header field
        x_host_header = fields[15] if len(fields) > 15 else ""

        suffix = f"{self.config.WORKSPACES_DOMAIN}"
        if not x_host_header.endswith(suffix):
            logger.info("Host '%s' doesn't end in '%s'; ignoring line", x_host_header, suffix)
            return None

        host_prefix = x_host_header[: -len(suffix)]
        workspace = host_prefix.split(".", 1)[0]

        if not workspace:
            logger.info(
                "Unable to determine workspace from host header '%s'; ignoring line.",
                x_host_header,
            )
            return None

        sku_enum = self.aws_classifier.classify(client_ip)
        sku = sku_enum.value

        aggregation_key = f"{log_filename}-{workspace}-{sku}"
        event_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, aggregation_key)

        timestamp_iso = f"{date}T{time_str}Z"

        return {
            "uuid": str(event_uuid),
            "workspace": workspace,
            "sku": sku,
            "data_size": sc_bytes,
            "timestamp": timestamp_iso,
            "aggregation_key": aggregation_key,
        }

    def run(self) -> None:
        logger.info("Running BillingScanner...")
        prefix = self.config.LOG_FOLDER
        if self.config.DISTRIBUTION_ID:
            prefix = f"{prefix}{self.config.DISTRIBUTION_ID}."
        with ScannerState(self.config.STATE_FILE) as state:
            start_after = state.last_processed or ""
            logger.info(
                "Listing from bucket=%s prefix=%s start_after=%r",
                self.config.S3_BUCKET,
                prefix,
                start_after,
            )
            all_keys = list_files(
                self.s3_client,
                self.config.S3_BUCKET,
                prefix,
                start_after=start_after,
            )
            logger.info(
                "Found %d file(s) in bucket %s under %s.",
                len(all_keys),
                self.config.S3_BUCKET,
                self.config.LOG_FOLDER,
            )

            new_keys = [k for k in all_keys if k > start_after]
            logger.info("Identified %d new file(s) to process.", len(new_keys))
            for key in new_keys:
                try:
                    success = self.process_log_file(key)
                except Exception as err:
                    logger.error("Error processing '%s': %s", key, err)
                    raise
                if not success:
                    logger.warning("No content in '%s', will retry later.", key)
                    break

                state.mark_last_processed(key)

        logger.info("BillingScanner run complete.")
