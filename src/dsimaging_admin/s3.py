"""S3/MinIO client helpers."""

import json
import re

import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
from urllib.parse import urlparse


def detect_backend(endpoint: str | None, override: str = "auto") -> tuple[str, str]:
    """Return ``(backend, rationale)`` for an endpoint and optional override."""
    if override and override != "auto":
        return override, f"explicit override: {override}"
    endpoint = (endpoint or "").strip()
    lower = endpoint.lower()
    if not lower or "amazonaws.com" in lower:
        return "aws", "empty endpoint or amazonaws.com uses native AWS S3"
    if (
        "minio" in lower
        or lower.startswith("http://127.0.0.1")
        or lower.startswith("http://localhost")
        or lower.startswith("https://127.0.0.1")
        or lower.startswith("https://localhost")
    ):
        return "minio", "local/minio endpoint heuristic"
    return "s3-compatible", "custom S3-compatible endpoint"


def create_client(endpoint: str | None, access_key: str | None = None,
                  secret_key: str | None = None,
                  region: str = "") -> boto3.client:
    """Create a boto3 S3 client.

    For MinIO (IP/localhost endpoints), region defaults to "us-east-1"
    as a dummy value that boto3 accepts.
    """
    effective_region = region if region else "us-east-1"
    kwargs = {"region_name": effective_region}
    if endpoint:
        kwargs["endpoint_url"] = endpoint

    if access_key:
        kwargs["aws_access_key_id"] = access_key
    if secret_key:
        kwargs["aws_secret_access_key"] = secret_key

    return boto3.client("s3", config=Config(signature_version="s3v4"), **kwargs)


def create_sqs_client(access_key: str | None = None,
                      secret_key: str | None = None,
                      region: str = "") -> boto3.client:
    """Create a boto3 SQS client, preserving the default credential chain."""
    kwargs = {"region_name": region or "us-east-1"}
    if access_key:
        kwargs["aws_access_key_id"] = access_key
    if secret_key:
        kwargs["aws_secret_access_key"] = secret_key
    return boto3.client("sqs", **kwargs)


def provision_aws_store(endpoint: str | None, bucket: str, region: str = "",
                        access_key: str | None = None,
                        secret_key: str | None = None,
                        kms_key: str | None = None,
                        s3_client=None, sqs_client=None) -> dict:
    """Provision an AWS S3-backed dsimaging store and SQS event queue.

    The operation is idempotent and returns a structured report with one row
    per provisioning step plus the resolved SQS queue URL and ARN.
    """
    region = region or "us-east-1"
    s3 = s3_client or create_client(endpoint or None, access_key, secret_key, region)
    sqs = sqs_client or create_sqs_client(access_key, secret_key, region)
    steps: list[dict] = []

    def step(name: str, status: str, detail: str) -> None:
        steps.append({"name": name, "status": status, "detail": detail})

    _ensure_bucket(s3, bucket, region, step)
    _ensure_bucket_versioning(s3, bucket, step)
    _ensure_bucket_encryption(s3, bucket, kms_key, step)
    queue_name = _event_queue_name(bucket)
    queue_url, queue_arn, queue_created = _ensure_sqs_queue(sqs, queue_name)
    step(
        "sqs_queue",
        "ok" if queue_created else "skipped",
        f"{queue_name} -> {queue_url}",
    )
    policy_changed = _ensure_sqs_policy(sqs, queue_url, queue_arn, bucket)
    step(
        "sqs_policy",
        "ok" if policy_changed else "skipped",
        f"bucket arn:aws:s3:::{bucket} allowed to send messages",
    )
    notification_changed = _ensure_bucket_notification(s3, bucket, queue_arn)
    step(
        "bucket_notification",
        "ok" if notification_changed else "skipped",
        f"s3:ObjectCreated:* and s3:ObjectRemoved:* -> {queue_arn}",
    )
    return {
        "backend": "aws",
        "bucket": bucket,
        "region": region,
        "sqs_queue_name": queue_name,
        "sqs_queue_url": queue_url,
        "sqs_queue_arn": queue_arn,
        "steps": steps,
        "ok": all(item["status"] in ("ok", "skipped") for item in steps),
    }


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


def _client_error_code(exc: Exception) -> str:
    if isinstance(exc, ClientError):
        return str(exc.response.get("Error", {}).get("Code", ""))
    return ""


def _ensure_bucket(s3, bucket: str, region: str, step) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
        step("bucket", "skipped", f"{bucket} already exists")
        return
    except Exception as e:
        code = _client_error_code(e)
        if code and code not in ("404", "NoSuchBucket", "NotFound"):
            # Some clients use 403 for existing buckets without access. Surface
            # that as a failure instead of masking a permissions problem.
            if code in ("403", "AccessDenied"):
                raise
    kwargs = {"Bucket": bucket}
    if region != "us-east-1":
        kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
    s3.create_bucket(**kwargs)
    step("bucket", "ok", f"{bucket} created")


def _ensure_bucket_versioning(s3, bucket: str, step) -> None:
    current = s3.get_bucket_versioning(Bucket=bucket).get("Status")
    if current == "Enabled":
        step("versioning", "skipped", "already enabled")
        return
    s3.put_bucket_versioning(
        Bucket=bucket,
        VersioningConfiguration={"Status": "Enabled"},
    )
    step("versioning", "ok", "enabled")


def _ensure_bucket_encryption(s3, bucket: str, kms_key: str | None, step) -> None:
    desired = (
        {"SSEAlgorithm": "aws:kms", "KMSMasterKeyID": kms_key}
        if kms_key else {"SSEAlgorithm": "AES256"}
    )
    current = None
    try:
        config = s3.get_bucket_encryption(Bucket=bucket)
        rules = config.get("ServerSideEncryptionConfiguration", {}).get("Rules", [])
        if rules:
            current = rules[0].get("ApplyServerSideEncryptionByDefault", {})
    except Exception as e:
        code = _client_error_code(e)
        if code not in (
            "ServerSideEncryptionConfigurationNotFoundError",
            "NoSuchBucket",
            "404",
        ):
            raise
    if current and _encryption_matches(current, desired):
        step("encryption", "skipped", "already configured")
        return
    config = {
        "Rules": [{
            "ApplyServerSideEncryptionByDefault": desired,
        }],
    }
    s3.put_bucket_encryption(
        Bucket=bucket,
        ServerSideEncryptionConfiguration=config,
    )
    step(
        "encryption",
        "ok",
        "SSE-KMS configured" if kms_key else "SSE-S3 AES256 configured",
    )


def _encryption_matches(current: dict, desired: dict) -> bool:
    if current.get("SSEAlgorithm") != desired.get("SSEAlgorithm"):
        return False
    if desired.get("SSEAlgorithm") == "aws:kms":
        return current.get("KMSMasterKeyID") == desired.get("KMSMasterKeyID")
    return True


def _event_queue_name(bucket: str) -> str:
    safe_bucket = re.sub(r"[^A-Za-z0-9_-]", "-", bucket)
    return f"dsimaging-store-{safe_bucket}-events"[:80]


def _ensure_sqs_queue(sqs, queue_name: str) -> tuple[str, str, bool]:
    created = False
    try:
        queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    except Exception as e:
        code = _client_error_code(e)
        if code and code not in ("AWS.SimpleQueueService.NonExistentQueue", "QueueDoesNotExist"):
            raise
        queue_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
        created = True
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["QueueArn"],
    )["Attributes"]
    return queue_url, attrs["QueueArn"], created


def _ensure_sqs_policy(sqs, queue_url: str, queue_arn: str, bucket: str) -> bool:
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["Policy"],
    ).get("Attributes", {})
    policy = _load_policy(attrs.get("Policy"))
    policy.setdefault("Version", "2012-10-17")
    statements = policy.setdefault("Statement", [])
    if isinstance(statements, dict):
        statements = [statements]
        policy["Statement"] = statements
    sid = f"DsimagingStoreS3Events{re.sub(r'[^A-Za-z0-9]', '', bucket)[:40]}"
    statement = {
        "Sid": sid,
        "Effect": "Allow",
        "Principal": {"Service": "s3.amazonaws.com"},
        "Action": "SQS:SendMessage",
        "Resource": queue_arn,
        "Condition": {"ArnEquals": {"aws:SourceArn": f"arn:aws:s3:::{bucket}"}},
    }
    for i, existing in enumerate(statements):
        if existing.get("Sid") == sid:
            if existing == statement:
                return False
            statements[i] = statement
            sqs.set_queue_attributes(
                QueueUrl=queue_url,
                Attributes={"Policy": json.dumps(policy, sort_keys=True)},
            )
            return True
    statements.append(statement)
    sqs.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={"Policy": json.dumps(policy, sort_keys=True)},
    )
    return True


def _load_policy(raw: str | None) -> dict:
    if not raw:
        return {"Version": "2012-10-17", "Statement": []}
    try:
        policy = json.loads(raw)
    except Exception:
        return {"Version": "2012-10-17", "Statement": []}
    if not isinstance(policy, dict):
        return {"Version": "2012-10-17", "Statement": []}
    return policy


def _ensure_bucket_notification(s3, bucket: str, queue_arn: str) -> bool:
    current = s3.get_bucket_notification_configuration(Bucket=bucket)
    notification = _clean_notification_config(current)
    queues = [
        item for item in notification.get("QueueConfigurations", [])
        if item.get("QueueArn") != queue_arn
    ]
    desired = {
        "Id": "dsimaging-store-events",
        "QueueArn": queue_arn,
        "Events": ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"],
    }
    if len(queues) != len(notification.get("QueueConfigurations", [])):
        existing = [
            item for item in notification.get("QueueConfigurations", [])
            if item.get("QueueArn") == queue_arn
        ][0]
        if _queue_notification_matches(existing, desired):
            return False
    queues.append(desired)
    notification["QueueConfigurations"] = queues
    s3.put_bucket_notification_configuration(
        Bucket=bucket,
        NotificationConfiguration=notification,
    )
    return True


def _clean_notification_config(config: dict) -> dict:
    allowed = (
        "TopicConfigurations",
        "QueueConfigurations",
        "LambdaFunctionConfigurations",
        "EventBridgeConfiguration",
    )
    return {key: value for key, value in config.items() if key in allowed and value}


def _queue_notification_matches(current: dict, desired: dict) -> bool:
    return (
        current.get("QueueArn") == desired["QueueArn"]
        and sorted(current.get("Events", [])) == sorted(desired["Events"])
        and current.get("Id") == desired["Id"]
        and not current.get("Filter")
    )


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
