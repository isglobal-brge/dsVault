"""S3/MinIO client wrapper."""

import boto3
from botocore.config import Config


def create_client(endpoint: str, access_key: str, secret_key: str,
                  region: str = "") -> boto3.client:
    """Create a boto3 S3 client.

    For MinIO (IP/localhost endpoints), region defaults to "us-east-1"
    as a dummy value that boto3 accepts.
    """
    effective_region = region if region else "us-east-1"
    kwargs = {"region_name": effective_region}
    if endpoint:
        kwargs["endpoint_url"] = endpoint

    return boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        **kwargs,
    )


def list_datasets(s3, bucket: str) -> list[dict]:
    """List published datasets in the bucket."""
    datasets = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix="datasets/", Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            ds_id = cp["Prefix"].strip("/").split("/")[-1]
            has_manifest = _object_exists(s3, bucket, f"datasets/{ds_id}/manifest.yaml")
            datasets.append({
                "dataset_id": ds_id,
                "status": "published" if has_manifest else "incomplete",
            })
    return datasets


def list_objects(s3, bucket: str, prefix: str) -> list[dict]:
    """List all objects under a prefix (with pagination)."""
    objects = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            objects.append({
                "key": obj["Key"],
                "size": obj["Size"],
                "last_modified": obj["LastModified"].isoformat(),
            })
    return objects


def _object_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False
