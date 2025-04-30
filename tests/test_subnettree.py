import json

import pytest

from billing_scanner.subnettree import AWSIPClassifier, EgressSKU


@pytest.fixture
def ip_data_file(tmp_path):
    data = {
        "prefixes": [
            # In-region prefixes (eu-west-2)
            {
                "service": "AMAZON",
                "region": "eu-west-2",
                "ip_prefix": "10.1.0.0/16",
                "ipv6_prefix": "2001:db8::/32",
            },
            # Other-region prefix
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


def test_ipv4_and_ipv6_classification(tmp_path, monkeypatch, ip_data_file):
    # Simulate network failure to force the fallback.
    monkeypatch.setattr(
        "billing_scanner.subnettree.requests.get",
        lambda url: (_ for _ in ()).throw(Exception("no network")),
    )

    clf = AWSIPClassifier(
        url="https://does-not-matter/",
        current_region="eu-west-2",
        fallback_file=ip_data_file,
    )

    # IPv4 tests
    assert clf.classify("10.1.2.3") == EgressSKU.REGION
    assert clf.classify("192.168.1.1") == EgressSKU.INTERREGION
    assert clf.classify("8.8.8.8") == EgressSKU.INTERNET

    # IPv6 tests
    assert clf.classify("2001:db8::1") == EgressSKU.REGION
    assert clf.classify("2001:db9::1") == EgressSKU.INTERNET
