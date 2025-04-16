import json
import logging
from enum import Enum
from typing import Tuple

import requests
import SubnetTree

logger = logging.getLogger(__name__)


class EgressSKU(Enum):
    REGION = "EGRESS-REGION"
    INTERREGION = "EGRESS-INTERREGION"
    INTERNET = "EGRESS-INTERNET"


class AWSIPClassifier:
    """
    AWSIPClassifier encapsulates the logic for loading AWS IP ranges and classifying a client IP
    into one of three egress categories (SKUs).

    Example usage:
        classifier = AWSIPClassifier(
            url="https://ip-ranges.amazonaws.com/ip-ranges.json",
            current_region="eu-west-2",
            fallback_file="fallback_ip_ranges.json"  # Optional fallback
        )
        sku = classifier.classify("52.93.153.170")  # Returns an EgressSKU enum member
    """

    def __init__(self, url: str, current_region: str, fallback_file: str = None):
        self.url = url
        self.current_region = current_region
        # Try loading from the remote URL...
        try:
            response = requests.get(self.url)
            response.raise_for_status()
            ip_data = response.json()
            logger.info("Successfully loaded AWS IP ranges from remote URL.")
        except Exception:
            logger.exception("Failed to load AWS IP ranges from remote URL.")
            # If a fallback file is provided, attempt to load it.
            if fallback_file:
                try:
                    with open(fallback_file, "r") as f:
                        ip_data = json.load(f)
                    logger.info("Successfully loaded AWS IP ranges from fallback file.")
                except Exception:
                    logger.exception("Failed to load AWS IP ranges from fallback file.")
                    raise Exception("Cannot load AWS IP ranges; aborting to prevent overcharging.")
            else:
                raise Exception("Cannot load AWS IP ranges and no fallback specified.")

        self.current_tree, self.aws_tree = self.build_trees(ip_data, current_region)

    @staticmethod
    def build_trees(
        ip_data: dict, current_region: str
    ) -> Tuple[SubnetTree.SubnetTree, SubnetTree.SubnetTree]:
        current_tree = SubnetTree.SubnetTree()
        aws_tree = SubnetTree.SubnetTree()
        for prefix in ip_data.get("prefixes", []):
            cidr = prefix.get("ip_prefix")
            region = prefix.get("region", "")
            if cidr:
                if region == current_region:
                    current_tree[cidr] = EgressSKU.REGION.value
                else:
                    aws_tree[cidr] = EgressSKU.INTERREGION.value
        return current_tree, aws_tree

    def classify(self, client_ip: str) -> EgressSKU:
        if client_ip in self.current_tree:
            return EgressSKU.REGION
        elif client_ip in self.aws_tree:
            return EgressSKU.INTERREGION
        else:
            return EgressSKU.INTERNET
