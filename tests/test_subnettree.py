import json
from pathlib import Path

import pytest

from billing_scanner.subnettree import AWSIPClassifier, EgressSKU


@pytest.fixture
def ip_data_file(tmp_path: Path) -> str:
    data = {
        "prefixes": [
            {
                "service": "AMAZON",
                "region": "eu-west-2",
                "ip_prefix": "10.1.0.0/16",
                "ipv6_prefix": "2001:db8::/32",
            },
            {
                "service": "AMAZON",
                "region": "us-east-1",
                "ip_prefix": "192.168.0.0/16",
                "ipv6_prefix": None,
            },
        ]
    }
    path = tmp_path / "ips.json"
    path.write_text(json.dumps(data))
    return str(path)


def test_ipv4_and_ipv6_classification(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ip_data_file: str) -> None:
    monkeypatch.setattr(
        "billing_scanner.subnettree.requests.get",
        lambda url: (_ for _ in ()).throw(Exception("no network")),
    )

    clf = AWSIPClassifier(
        url="https://does-not-matter/",
        current_region="eu-west-2",
        fallback_file=ip_data_file,
    )

    assert clf.classify("10.1.2.3") == EgressSKU.REGION
    assert clf.classify("192.168.1.1") == EgressSKU.INTERREGION
    assert clf.classify("8.8.8.8") == EgressSKU.INTERNET

    assert clf.classify("2001:db8::1") == EgressSKU.REGION
    assert clf.classify("2001:db9::1") == EgressSKU.INTERNET
