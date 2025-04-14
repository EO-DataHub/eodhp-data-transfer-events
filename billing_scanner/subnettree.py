import logging
from typing import Tuple

import requests
import SubnetTree

logger = logging.getLogger(__name__)


def load_aws_ip_ranges(url: str = None) -> dict:
    """
    Load AWS IP ranges JSON from a URL.
    The URL is read from the environment variable 'AWS_IP_RANGES_URL' if not provided.
    By default, it falls back to "https://ip-ranges.amazonaws.com/ip-ranges.json".
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception:
        logger.exception("Failed to load AWS IP ranges from %s", url)
        raise


def build_subnet_trees(
    ip_data: dict, current_region: str
) -> Tuple[SubnetTree.SubnetTree, SubnetTree.SubnetTree]:
    """
    Build two SubnetTree objects:
      - current_tree: Contains CIDR ranges for the given current_region.
      - aws_tree: Contains CIDR ranges for all AWS IPs outside the current_region.

    Only considers entries with service == "AMAZON".
    """
    current_tree = SubnetTree.SubnetTree()
    aws_tree = SubnetTree.SubnetTree()

    for prefix in ip_data.get("prefixes", []):
        if prefix.get("service") != "AMAZON":
            continue
        cidr = prefix.get("ip_prefix")
        region = prefix.get("region", "")
        if cidr:
            if region == current_region:
                current_tree[cidr] = "EGRESS-REGION"
            else:
                aws_tree[cidr] = "EGRESS-INTERREGION"
    return current_tree, aws_tree
