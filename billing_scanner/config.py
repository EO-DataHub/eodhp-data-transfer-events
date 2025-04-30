import os


class Config:
    def __init__(self):
        self.S3_BUCKET = os.getenv("S3_BUCKET", "access-log-eodhp-dev")
        self.LOG_FOLDER = os.getenv("LOG_FOLDER", "")
        self.STATE_FILE = os.getenv("STATE_FILE", "processed_logs.json")
        self.PULSAR_TOPIC = os.getenv("PULSAR_TOPIC", "billing-events")
        self.AWS_REGION = os.getenv("AWS_REGION", "eu-west-2")
        self.AWS_IP_RANGES_URL = os.getenv(
            "AWS_IP_RANGES_URL", "https://ip-ranges.amazonaws.com/ip-ranges.json"
        )
        self.FALLBACK_IP_RANGES_FILE = os.getenv(
            "FALLBACK_IP_RANGES_FILE", "/mnt/state/fallback_ip_ranges.json"
        )
        self.DISTRIBUTION_ID = os.getenv("DISTRIBUTION_ID", "")
        self.WORKSPACES_DOMAIN = os.getenv("WORKSPACES_DOMAIN", "eodatahub-workspaces.org.uk")
