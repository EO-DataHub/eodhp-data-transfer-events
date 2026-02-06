import json
import logging
from enum import Enum

import requests
import SubnetTree

logger = logging.getLogger(__name__)


class EgressSKU(Enum):
    REGION = "EGRESS-REGION"
    INTERREGION = "EGRESS-INTERREGION"
    INTERNET = "EGRESS-INTERNET"


class AWSIPClassifier:
    """AWSIPClassifier encapsulates the logic for loading AWS IP ranges and classifying a client IP
    into one of three egress categories (SKUs).
    """

    def __init__(self, url: str, current_region: str, fallback_file: str | None = None) -> None:
        self.url = url
        self.current_region = current_region
        try:
            response = requests.get(self.url)
            response.raise_for_status()
            ip_data = response.json()
            logger.info("Successfully loaded AWS IP ranges from remote URL %s.", self.url)
        except Exception:
            logger.exception("Failed to load AWS IP ranges from remote URL %s.", self.url)
            if fallback_file:
                try:
                    with open(fallback_file) as f:
                        ip_data = json.load(f)
                    logger.info("Successfully loaded AWS IP ranges from fallback file.")
                except Exception:
                    logger.exception("Failed to load AWS IP ranges from fallback file.")
                    raise Exception("Cannot load AWS IP ranges; aborting to prevent overcharging.") from None
            else:
                raise Exception("Cannot load AWS IP ranges and no fallback specified.") from None

        self.current_tree, self.aws_tree = self.build_trees(ip_data, current_region)

    @staticmethod
    def build_trees(
        ip_data: dict[str, list[dict[str, str]]],
        current_region: str,
    ) -> tuple[SubnetTree.SubnetTree, SubnetTree.SubnetTree]:
        """Builds two SubnetTree objects for current-region and other-region prefixes."""
        current_tree = SubnetTree.SubnetTree()
        aws_tree = SubnetTree.SubnetTree()
        for prefix in ip_data.get("prefixes", []):
            region = prefix.get("region", "")

            cidr4 = prefix.get("ip_prefix")
            if cidr4:
                if region == current_region:
                    current_tree[cidr4] = EgressSKU.REGION.value
                else:
                    aws_tree[cidr4] = EgressSKU.INTERREGION.value

            cidr6 = prefix.get("ipv6_prefix")
            if cidr6:
                if region == current_region:
                    current_tree[cidr6] = EgressSKU.REGION.value
                else:
                    aws_tree[cidr6] = EgressSKU.INTERREGION.value

        return current_tree, aws_tree

    def classify(self, client_ip: str) -> EgressSKU:
        if client_ip in self.current_tree:
            return EgressSKU.REGION
        elif client_ip in self.aws_tree:
            return EgressSKU.INTERREGION
        else:
            return EgressSKU.INTERNET
