import os


class Config:
    def __init__(self):
        self.S3_BUCKET = os.getenv("S3_BUCKET", "eodh-access-logs-cluster")
        self.LOG_FOLDER = os.getenv("LOG_FOLDER", "AWSLogs/312280911266/CloudFront/workspace/")
        self.STATE_FILE = os.getenv("STATE_FILE", "/mnt/state/processed_logs.json")
        self.PULSAR_BROKER_URL = os.getenv("PULSAR_URL", "pulsar://pulsar-proxy:6650")
        self.PULSAR_TOPIC = os.getenv("PULSAR_TOPIC", "billing-events")
        self.AWS_REGION = os.getenv("AWS_REGION", "eu-west-2")
        self.AWS_IP_RANGES_URL = os.getenv(
            "AWS_IP_RANGES_URL", "https://ip-ranges.amazonaws.com/ip-ranges.json"
        )
        self.DISTRIBUTION_ID = os.getenv("DISTRIBUTION_ID", "")
