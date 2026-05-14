"""Dataset verification helpers."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, asdict

import pyarrow as pa
import pyarrow.parquet as pq

from .hashing import is_image_file
from .s3 import get_object_bytes, head_object, list_objects, parse_s3_uri


@dataclass
class VerifyIssue:
    sample_id: str
    uri: str
    issue: str
    detail: str


@dataclass
class VerifyResult:
    dataset_id: str
    checked: int
    skipped: int
    missing: int
    mismatched: int
    extra: int
    quick_ok: int
    issues: list[VerifyIssue]

    @property
    def ok(self) -> bool:
        return not self.issues

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["ok"] = self.ok
        return payload


def verify_dataset(s3, bucket: str, dataset_id: str,
                   sample_fraction: float = 1.0,
                   quick: bool = False) -> VerifyResult:
    """Verify S3 objects against dataset hash indexes."""
    if sample_fraction <= 0 or sample_fraction > 1:
        raise ValueError("sample_fraction must be > 0 and <= 1")

    prefix = f"datasets/{dataset_id}"
    issues: list[VerifyIssue] = []
    checked = 0
    skipped = 0
    quick_ok = 0
    represented_keys: set[str] = set()

    tables = []
    main_table = _read_parquet_object(s3, bucket, f"{prefix}/indexes/content_hash_index.parquet")
    tables.append(("images", main_table))
    try:
        mask_table = _read_parquet_object(
            s3, bucket, f"{prefix}/indexes/masks_content_hash_index.parquet"
        )
        tables.append(("masks", mask_table))
    except Exception:
        mask_table = None

    manifest_rows = _read_sample_manifest_rows(s3, bucket, prefix)

    for asset_name, table in tables:
        rows = table.to_pylist()
        selected = _sample_rows(rows, sample_fraction)
        skipped += len(rows) - len(selected)
        for row in selected:
            checked += 1
            row_issue, keys, used_quick = _verify_row(
                s3, bucket, row, manifest_rows, quick=quick,
            )
            represented_keys.update(keys)
            if used_quick:
                quick_ok += 1
            if row_issue:
                issues.append(row_issue)

    for asset_name, source_prefix in (
        ("images", f"{prefix}/source/images/"),
        ("masks", f"{prefix}/source/masks/"),
    ):
        if asset_name == "masks" and mask_table is None:
            continue
        for obj in list_objects(s3, bucket, source_prefix):
            rel = obj["key"][len(source_prefix):]
            if not rel or rel.endswith("/"):
                continue
            filename = rel.rsplit("/", 1)[-1]
            if is_image_file(filename) and obj["key"] not in represented_keys:
                issues.append(VerifyIssue(
                    sample_id="",
                    uri=f"s3://{bucket}/{obj['key']}",
                    issue="extra",
                    detail="object is present under source but absent from hash index",
                ))

    missing = sum(1 for issue in issues if issue.issue == "missing")
    mismatched = sum(1 for issue in issues if issue.issue == "mismatch")
    extra = sum(1 for issue in issues if issue.issue == "extra")
    return VerifyResult(
        dataset_id=dataset_id,
        checked=checked,
        skipped=skipped,
        missing=missing,
        mismatched=mismatched,
        extra=extra,
        quick_ok=quick_ok,
        issues=issues,
    )


def _read_parquet_object(s3, bucket: str, key: str) -> pa.Table:
    data = get_object_bytes(s3, bucket, key)
    return pq.read_table(pa.BufferReader(data))


def _read_sample_manifest_rows(s3, bucket: str, prefix: str) -> dict[str, dict]:
    try:
        table = _read_parquet_object(s3, bucket, f"{prefix}/metadata/sample_manifests.parquet")
    except Exception:
        return {}
    return {row.get("sample_id"): row for row in table.to_pylist()}


def _sample_rows(rows: list[dict], fraction: float) -> list[dict]:
    if fraction >= 1 or not rows:
        return rows
    count = max(1, int(math.ceil(len(rows) * fraction)))
    return sorted(
        rows,
        key=lambda row: hashlib.sha256(str(row.get("sample_id", "")).encode()).hexdigest(),
    )[:count]


def _verify_row(s3, configured_bucket: str, row: dict,
                manifest_rows: dict[str, dict], quick: bool) -> tuple[VerifyIssue | None, set[str], bool]:
    uri = row.get("uri") or ""
    sample_id = row.get("sample_id") or ""
    expected_hash = row.get("content_hash") or ""
    expected_size = int(row.get("size") or 0)
    expected_etag = row.get("etag")

    try:
        uri_bucket, key = parse_s3_uri(uri)
    except ValueError as e:
        return VerifyIssue(sample_id, uri, "mismatch", str(e)), set(), False
    bucket = uri_bucket or configured_bucket

    source_kind = row.get("source_kind")
    if source_kind == "dicom_series" or key.endswith("/"):
        return _verify_dicom_series(
            s3, bucket, key, sample_id, uri, expected_hash,
            expected_size, expected_etag, quick,
        )

    meta = head_object(s3, bucket, key)
    if not meta:
        return VerifyIssue(sample_id, uri, "missing", "object is absent"), {key}, False
    if quick and expected_etag and meta.get("etag") == expected_etag and meta.get("size") == expected_size:
        return None, {key}, True

    actual_hash = _sha256_s3_object(s3, bucket, key)
    if actual_hash != expected_hash:
        return VerifyIssue(
            sample_id, uri, "mismatch",
            f"sha256 {actual_hash} != recorded {expected_hash}",
        ), {key}, False
    return None, {key}, False


def _verify_dicom_series(s3, bucket: str, prefix_key: str, sample_id: str,
                         uri: str, expected_hash: str, expected_size: int,
                         expected_etag: str | None, quick: bool
                         ) -> tuple[VerifyIssue | None, set[str], bool]:
    prefix = prefix_key if prefix_key.endswith("/") else f"{prefix_key}/"
    objects = [
        obj for obj in list_objects(s3, bucket, prefix)
        if obj["key"].lower().endswith(".dcm")
    ]
    if not objects:
        return VerifyIssue(sample_id, uri, "missing", "DICOM series has no .dcm objects"), set(), False
    keys = {obj["key"] for obj in objects}
    total_size = sum(int(obj.get("size") or 0) for obj in objects)
    etag_join = ",".join(obj.get("etag") or "" for obj in sorted(objects, key=lambda item: item["key"]))
    if quick and expected_etag and etag_join == expected_etag and total_size == expected_size:
        return None, keys, True

    h = hashlib.sha256()
    for obj in sorted(objects, key=lambda item: item["key"]):
        h.update(_sha256_s3_object(s3, bucket, obj["key"]).encode())
    actual_hash = h.hexdigest()
    if actual_hash != expected_hash:
        return VerifyIssue(
            sample_id, uri, "mismatch",
            f"series sha256 {actual_hash} != recorded {expected_hash}",
        ), keys, False
    return None, keys, False


def _sha256_s3_object(s3, bucket: str, key: str) -> str:
    h = hashlib.sha256()
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    try:
        for chunk in iter(lambda: body.read(65536), b""):
            if chunk:
                h.update(chunk)
    finally:
        body.close()
    return h.hexdigest()
