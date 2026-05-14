"""S3/MinIO client helpers."""

import boto3
from botocore.config import Config
from urllib.parse import urlparse


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
            if not _prefix_has_current_objects(s3, bucket, f"datasets/{ds_id}/"):
                continue
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
                "etag": obj.get("ETag", "").strip('"') or None,
                "version_id": None,
            })
    return objects


def head_object(s3, bucket: str, key: str) -> dict | None:
    """Return S3 object metadata, or ``None`` when the object is absent."""
    try:
        resp = s3.head_object(Bucket=bucket, Key=key)
    except Exception:
        return None
    return {
        "key": key,
        "size": int(resp.get("ContentLength", 0)),
        "last_modified": resp.get("LastModified").isoformat()
        if resp.get("LastModified") else None,
        "etag": resp.get("ETag", "").strip('"') or None,
        "version_id": resp.get("VersionId"),
        "content_type": resp.get("ContentType"),
    }


def get_object_bytes(s3, bucket: str, key: str) -> bytes:
    """Read one S3 object fully and close the streaming body."""
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    try:
        return body.read()
    finally:
        body.close()


def put_object_bytes(s3, bucket: str, key: str, data: bytes,
                     content_type: str | None = None) -> None:
    """Write bytes to S3."""
    kwargs = {"Bucket": bucket, "Key": key, "Body": data}
    if content_type:
        kwargs["ContentType"] = content_type
    s3.put_object(**kwargs)


def copy_object(s3, bucket: str, source_key: str, dest_key: str) -> None:
    """Copy an object inside the same bucket."""
    s3.copy_object(
        Bucket=bucket,
        Key=dest_key,
        CopySource={"Bucket": bucket, "Key": source_key},
    )


def delete_keys(s3, bucket: str, keys: list[str]) -> int:
    """Delete current object versions for the provided keys in batches."""
    deleted = 0
    for i in range(0, len(keys), 1000):
        chunk = keys[i:i + 1000]
        if not chunk:
            continue
        resp = s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in chunk], "Quiet": True},
        )
        deleted += len(chunk) - len(resp.get("Errors", []))
    return deleted


def list_object_versions(s3, bucket: str, prefix: str) -> list[dict]:
    """List object versions and delete markers under a prefix."""
    versions = []
    paginator = s3.get_paginator("list_object_versions")
    try:
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for page in pages:
            for obj in page.get("Versions", []):
                versions.append({
                    "key": obj["Key"],
                    "version_id": obj.get("VersionId"),
                    "is_delete_marker": False,
                })
            for obj in page.get("DeleteMarkers", []):
                versions.append({
                    "key": obj["Key"],
                    "version_id": obj.get("VersionId"),
                    "is_delete_marker": True,
                })
    except Exception:
        return []
    return versions


def delete_object_versions(s3, bucket: str, versions: list[dict]) -> int:
    """Delete explicit S3 object versions/delete markers in batches."""
    deleted = 0
    for i in range(0, len(versions), 1000):
        chunk = versions[i:i + 1000]
        objects = [
            {"Key": item["key"], "VersionId": item["version_id"]}
            for item in chunk
            if item.get("version_id")
        ]
        if not objects:
            continue
        resp = s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": objects, "Quiet": True},
        )
        deleted += len(objects) - len(resp.get("Errors", []))
    return deleted


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Return ``(bucket, key)`` for an ``s3://bucket/key`` URI."""
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"not an S3 URI: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def _object_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def _prefix_has_current_objects(s3, bucket: str, prefix: str) -> bool:
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if _object_exists(s3, bucket, obj["Key"]):
                return True
    return False
