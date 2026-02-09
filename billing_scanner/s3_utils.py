import gzip
import logging
from typing import Protocol

from eodhp_utils.runner import get_boto3_session

logger = logging.getLogger(__name__)


class S3Client(Protocol):
    def get_paginator(self, operation_name: str) -> object: ...
    def get_object(self, **kwargs: str) -> dict[str, object]: ...


def init_s3_resource(region: str) -> object:
    return get_boto3_session().resource("s3", region_name=region)


def init_s3_client(region: str) -> S3Client:
    return get_boto3_session().client("s3", region_name=region)


def list_files(s3_client: S3Client, bucket: str, prefix: str, start_after: str | None = None) -> list[str]:
    """List all S3 object keys under the given prefix."""
    paginator = s3_client.get_paginator("list_objects_v2")
    kwargs: dict[str, str] = {"Bucket": bucket, "Prefix": prefix}
    if start_after:
        kwargs["StartAfter"] = start_after
    file_keys = []
    for page in paginator.paginate(**kwargs):
        for obj in page.get("Contents", []):
            file_keys.append(obj["Key"])
    return file_keys


def download_file(s3_client: S3Client, bucket: str, key: str) -> str:
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read()
        if key.endswith(".gz"):
            body = gzip.decompress(body).decode("utf-8")
        else:
            body = body.decode("utf-8")
        return body
    except Exception as e:
        logger.exception("Failed to download file from bucket '%s' with key '%s': %s", bucket, key, e)
        raise
