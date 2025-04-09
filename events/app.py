import fcntl
import gzip
import json
import logging
import os
import sys

import boto3
from pulsar import Client

logger = logging.getLogger(__name__)


class BillingScanner:
    def __init__(self, **kwargs):
        # Configuration from kwargs or environment variables.
        self.s3_bucket = kwargs.get("s3_bucket", os.getenv("S3_BUCKET", "acconting-events"))
        if not self.s3_bucket:
            raise KeyError("S3_BUCKET not set")
        self.log_folder = kwargs.get(
            "log_folder", os.getenv("LOG_FOLDER", "AWSLogs/312280911266/CloudFront/accounting/")
        )
        self.state_file = kwargs.get(
            "state_file", os.getenv("STATE_FILE", "/mnt/state/processed_logs.json")
        )
        self.pulsar_broker = kwargs.get(
            "pulsar_broker", os.getenv("PULSAR_URL", "pulsar://pulsar-proxy.pulsar:6650")
        )
        self.pulsar_topic = kwargs.get("pulsar_topic", os.getenv("PULSAR_TOPIC", "accounting"))
        self.aws_region = kwargs.get("aws_region", os.getenv("AWS_REGION", "eu-west-2"))

        self.s3 = None
        self.initialise_s3()
        self.s3_client = boto3.client("s3", region_name=self.aws_region)
        self.processed_files = self.load_processed_files()

    def initialise_s3(self):
        """
        Initialize the boto3 S3 resource using the service account's IAM role.
        Since no AWS_ACCESS_KEY or AWS_SECRET_ACCESS_KEY is provided, boto3 uses the pod's IAM role.
        """
        self.s3 = boto3.resource("s3", region_name=self.aws_region)

    # --- File Locking Helpers ---
    def acquire_lock(self, fp):
        fcntl.flock(fp, fcntl.LOCK_EX)

    def release_lock(self, fp):
        fcntl.flock(fp, fcntl.LOCK_UN)

    def load_processed_files(self):
        """
        Load the set of processed S3 log file keys from the persistent JSON state file.
        This prevents reprocessing the same file.
        """
        if not os.path.exists(self.state_file):
            with open(self.state_file, "w") as f:
                json.dump({"processed": []}, f)
        with open(self.state_file, "r") as f:
            self.acquire_lock(f)
            try:
                data = json.load(f)
                processed = set(data.get("processed", []))
            except json.JSONDecodeError:
                processed = set()
            finally:
                self.release_lock(f)
        return processed

    def save_processed_files(self):
        """
        Save the updated set of processed file keys back to the persistent state file.
        """
        with open(self.state_file, "w") as f:
            self.acquire_lock(f)
            json.dump({"processed": list(self.processed_files)}, f, indent=2)
            self.release_lock(f)

    def list_log_files(self):
        """
        List all log file keys under the specified LOG_FOLDER in the S3 bucket.
        """
        paginator = self.s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.s3_bucket, Prefix=self.log_folder)
        file_keys = []
        for page in pages:
            for obj in page.get("Contents", []):
                file_keys.append(obj["Key"])
        return file_keys

    def download_log_file(self, key):
        """
        Download the content of a log file from S3.
        If the file is gzipped, decompress it.
        """
        try:
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=key)
            body = response["Body"].read()
            if key.endswith(".gz"):
                body = gzip.decompress(body).decode("utf-8")
            else:
                body = body.decode("utf-8")
            return body
        except Exception as e:
            logger.error(f"Failed to download {key}: {e}")
            return None

    def process_log_line(self, line):
        """
        Parse a CloudFront log line and generate a billing event payload.

        Using the log header provided, our field indices are:
            0: timestamp (UNIX timestamp, not used in event building)
            1: DistributionId
            2: date (e.g., 2025-04-07)
            3: time (e.g., 13:38:48)
            4: x-edge-location
            5: sc-bytes (data transferred)
            6: c-ip
            7: cs-method
            8: cs(Host) → our domain value
            9: cs-uri-stem → URL path

        We create an event with:
         - workspace_id (mapped from cs(Host))
         - data_size (from sc-bytes)
         - timestamp (combination of date and time)
         - destination (derived from the cs-uri-stem, can be extended)
         - event_id for uniqueness.
        """
        if line.startswith("#"):
            return None  # Skip header lines
        fields = line.split()
        try:
            date = fields[2]  # e.g., "2025-04-07"
            time_str = fields[3]  # e.g., "13:38:48"
            sc_bytes = fields[5]  # e.g., "398"
            cs_host = fields[8]  # e.g., "dsakofwkmfc6v.cloudfront.net"
            cs_uri_stem = fields[9]  # e.g., "/notebooks/user/tjellicoe-tpzuk/api/sessions"

            # Map the domain to a workspace ID.
            workspace_id = self.map_domain_to_workspace(cs_host)
            if not workspace_id:
                logger.info(f"No workspace mapping for domain {cs_host}; skipping line.")
                return None

            event_id = f"{workspace_id}-{date}-{time_str}-{cs_uri_stem}-{sc_bytes}"
            event = {
                "event_id": event_id,
                "workspace_id": workspace_id,
                "destination": self.determine_destination(cs_uri_stem),
                "data_size": int(sc_bytes),
                "timestamp": f"{date}T{time_str}Z",
            }
            return event
        except Exception as e:
            logger.error(f"Error parsing line: {line[:50]} ... Error: {e}")
            return None

    def map_domain_to_workspace(self, domain):
        """
        Map the CloudFront domain (cs(Host)) to a workspace ID.
        Replace this mapping with your own logic as needed.
        """
        mapping = {
            "dsakofwkmfc6v.cloudfront.net": "workspace1",
            # Add more mappings as needed...
        }
        return mapping.get(domain)

    def determine_destination(self, uri):
        """
        Determine the destination category from the URI.
        This example uses a static value, but you can expand it
        to inspect the URI (for example, to differentiate API endpoints).
        """
        return "current-region"

    def publish_event(self, event):
        """
        Publish the billing event to Pulsar.
        """
        try:
            client = Client(self.pulsar_broker)
            producer = client.create_producer(self.pulsar_topic)
            message = json.dumps(event)
            producer.send(message.encode("utf-8"))
            producer.close()
            client.close()
            logger.info(f"Published event {event['event_id']} to Pulsar.")
            return True
        except Exception as e:
            logger.error(f"Error publishing event: {e}")
            return False

    def process_log_file(self, key):
        """
        Process a single log file from S3:
          - Download and, if necessary, decompress the file.
          - For each log line, parse and publish billing events.
        """
        logger.info(f"Processing log file: {key}")
        content = self.download_log_file(key)
        if not content:
            return False
        for line in content.splitlines():
            event = self.process_log_line(line)
            if event:
                self.publish_event(event)
        return True

    def run(self):
        """
        List available S3 log files, filter out those already processed,
        process new files, and update the persistent state file.
        """
        logger.info("Running BillingScanner...")
        all_files = self.list_log_files()
        logger.info(
            f"Found {len(all_files)} file(s) in bucket {self.s3_bucket} under {self.log_folder}."
        )
        new_files = [key for key in all_files if key not in self.processed_files]
        logger.info(f"Identified {len(new_files)} new file(s) to process.")
        for key in new_files:
            if self.process_log_file(key):
                self.processed_files.add(key)
            else:
                logger.error(f"Failed to process file {key}; it will be retried later.")
        self.save_processed_files()
        logger.info("BillingScanner run complete.")

    def finish(self):
        logger.info("Finishing up BillingScanner.")


def main():
    scanner = BillingScanner()
    scanner.run()
    scanner.finish()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        main()
    except Exception as ex:
        logger.error(f"Unexpected error: {ex}")
        sys.exit(1)
