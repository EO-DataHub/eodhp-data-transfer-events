import gzip
import logging

import boto3

logger = logging.getLogger(__name__)


def init_s3_resource(region: str):
    return boto3.resource("s3", region_name=region)


def init_s3_client(region: str):
    return boto3.client("s3", region_name=region)


def list_files(s3_client, bucket: str, prefix: str) -> list:
    """
    List all S3 object keys under the given prefix.
    If DISTRIBUTION_ID is set in the configuration, the prefix is adjusted.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    file_keys = []
    for page in pages:
        for obj in page.get("Contents", []):
            file_keys.append(obj["Key"])
    return file_keys


def download_file(s3_client, bucket: str, key: str) -> str:
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read()
        if key.endswith(".gz"):
            body = gzip.decompress(body).decode("utf-8")
        else:
            body = body.decode("utf-8")
        return body
    except Exception as e:
        logging.exception(f"Failed to download {e}")
        raise
