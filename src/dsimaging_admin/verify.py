"""Dataset verification helpers."""

from __future__ import annotations

import hashlib
import json
import math
import secrets
from dataclasses import dataclass, asdict

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from .hashing import is_image_file
from .manifest import (
    metadata_contract_from_manifest,
    validate_dataset_id,
    validate_manifest_scope,
    validate_samples_metadata,
)
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
    dataset_id = validate_dataset_id(dataset_id)
    if sample_fraction <= 0 or sample_fraction > 1:
        raise ValueError("sample_fraction must be > 0 and <= 1")

    prefix = f"datasets/{dataset_id}"
    issues: list[VerifyIssue] = []
    checked = 0
    skipped = 0
    quick_ok = 0
    represented_keys: set[str] = set()
    sampling_key = secrets.token_bytes(32)

    tables = []
    main_table = _read_parquet_object(s3, bucket, f"{prefix}/indexes/content_hash_index.parquet")
    tables.append(("images", main_table))
    issues.extend(_publication_contract_issues(
        s3, bucket, prefix, main_table))
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
        selected = _sample_rows(rows, sample_fraction, sampling_key)
        skipped += len(rows) - len(selected)
        for row in rows:
            represented_keys.update(_represented_keys(s3, bucket, row))
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


def _publication_contract_issues(s3, bucket: str, prefix: str,
                                 index: pa.Table) -> list[VerifyIssue]:
    uri = f"s3://{bucket}/{prefix}/manifest.yaml"
    try:
        if head_object(s3, bucket, f"{prefix}/.publish-lock"):
            raise ValueError("dataset publication is still in progress")
        manifest = yaml.safe_load(get_object_bytes(
            s3, bucket, f"{prefix}/manifest.yaml"))
        validate_manifest_scope(manifest, bucket, prefix)
        contract = metadata_contract_from_manifest(manifest)
        metadata = _read_parquet_object(
            s3, bucket, f"{prefix}/metadata/samples.parquet")
        metadata = validate_samples_metadata(
            metadata, contract["privacy_unit_col"], contract.get("label_col"),
            contract.get("label_levels"))
        sample_manifests = _read_parquet_object(
            s3, bucket, f"{prefix}/metadata/sample_manifests.parquet")
        index_ids = index["sample_id"].to_pylist()
        metadata_ids = metadata["sample_id"].to_pylist()
        manifest_ids = sample_manifests["sample_id"].to_pylist()
        if (len(index_ids) != len(set(index_ids)) or
                len(manifest_ids) != len(set(manifest_ids)) or
                set(index_ids) != set(metadata_ids) or
                set(index_ids) != set(manifest_ids)):
            raise ValueError("published sample rosters do not match exactly")
        _validate_image_tables(
            index, sample_manifests, metadata,
            manifest["assets"]["images"]["uri"],
        )
        mask_asset = (manifest.get("assets") or {}).get("masks")
        mask_objects = [
            obj for obj in list_objects(
                s3, bucket, f"{prefix}/source/masks/")
            if is_image_file(obj["key"].rsplit("/", 1)[-1])
        ]
        mask_index_key = f"{prefix}/indexes/masks_content_hash_index.parquet"
        if mask_asset is None and (
                mask_objects or head_object(s3, bucket, mask_index_key)):
            raise ValueError("mask sources or index are not declared by the manifest")
        if mask_asset is not None:
            mask_index = _read_parquet_object(
                s3, bucket, mask_index_key)
            mask_ids = mask_index["sample_id"].to_pylist()
            if (len(mask_ids) != len(set(mask_ids)) or
                    not set(mask_ids).issubset(set(index_ids)) or
                    len(mask_ids) != len(mask_objects)):
                raise ValueError("mask roster is duplicate, orphaned, or incomplete")
            _validate_mask_table(
                mask_index, manifest["assets"]["masks"]["uri"])
    except Exception as exc:
        return [VerifyIssue(
            sample_id="", uri=uri, issue="mismatch",
            detail=f"publication contract is incomplete or invalid: {exc}",
        )]
    return []


def _validate_image_tables(index: pa.Table, sample_manifests: pa.Table,
                           metadata: pa.Table, image_root: str) -> None:
    required_index = {"sample_id", "uri", "content_hash", "size", "source_kind"}
    required_manifests = {
        "sample_id", "source_kind", "primary_uri", "files_json",
        "content_hash", "n_files",
    }
    if not required_index.issubset(index.column_names):
        raise ValueError("content hash index schema is incomplete")
    if not required_manifests.issubset(sample_manifests.column_names):
        raise ValueError("sample manifests schema is incomplete")

    index_rows = {row["sample_id"]: row for row in index.to_pylist()}
    manifest_rows = {
        row["sample_id"]: row for row in sample_manifests.to_pylist()
    }
    metadata_rows = {row["sample_id"]: row for row in metadata.to_pylist()}
    seen_uris = set()
    for sample_id, row in index_rows.items():
        sample_manifest = manifest_rows[sample_id]
        metadata_row = metadata_rows[sample_id]
        source_kind = row.get("source_kind")
        content_hash = str(row.get("content_hash") or "").lower()
        if source_kind not in {"single_file", "dicom_series"}:
            raise ValueError("content hash index has an invalid source_kind")
        if (sample_manifest.get("source_kind") != source_kind or
                str(sample_manifest.get("content_hash") or "").lower() != content_hash or
                not _is_sha256(content_hash)):
            raise ValueError("sample manifests do not match the content hash index")
        if ("source_kind" in metadata_row and
                metadata_row["source_kind"] != source_kind):
            raise ValueError("samples metadata source_kind is inconsistent")

        size = _nonnegative_integer(row.get("size"), "content hash index size")
        if size < 0:
            raise ValueError("content hash index size is invalid")
        n_files = _positive_integer(
            sample_manifest.get("n_files"), "sample manifest n_files")
        if ("n_files" in metadata_row and
                _positive_integer(metadata_row["n_files"], "metadata n_files") != n_files):
            raise ValueError("samples metadata n_files is inconsistent")

        uri = row.get("uri")
        relative = _relative_collection_uri(uri, image_root)
        if uri in seen_uris:
            raise ValueError("content hash index has duplicate URIs")
        seen_uris.add(uri)
        try:
            files = json.loads(sample_manifest.get("files_json") or "")
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError("sample manifest files_json is invalid") from exc
        if not isinstance(files, list) or len(files) != n_files:
            raise ValueError("sample manifest file roster is incomplete")
        paths = []
        for item in files:
            if not isinstance(item, dict) or not _safe_relative_path(item.get("path")):
                raise ValueError("sample manifest file path is invalid")
            paths.append(item["path"])
        if len(paths) != len(set(paths)):
            raise ValueError("sample manifest file roster has duplicate paths")

        primary_uri = sample_manifest.get("primary_uri")
        if source_kind == "single_file":
            if (n_files != 1 or primary_uri != relative or paths != [relative] or
                    files[0].get("role") != "primary"):
                raise ValueError("single-file sample manifest is inconsistent")
        else:
            relative = relative.rstrip("/")
            if (primary_uri not in (None, "") or
                    any(not path.startswith(f"{relative}/") for path in paths) or
                    any(item.get("role") != "slice" for item in files)):
                raise ValueError("DICOM sample manifest is inconsistent")


def _validate_mask_table(index: pa.Table, mask_root: str) -> None:
    required = {"sample_id", "uri", "content_hash", "size", "source_kind"}
    if not required.issubset(index.column_names):
        raise ValueError("mask content hash index schema is incomplete")
    seen_uris = set()
    for row in index.to_pylist():
        if row.get("source_kind") != "mask_file":
            raise ValueError("mask content hash index has an invalid source_kind")
        if not _is_sha256(str(row.get("content_hash") or "").lower()):
            raise ValueError("mask content hash index has an invalid content hash")
        _nonnegative_integer(row.get("size"), "mask content hash index size")
        uri = row.get("uri")
        relative = _relative_collection_uri(uri, mask_root)
        if not relative or relative.endswith("/") or uri in seen_uris:
            raise ValueError("mask content hash index has an invalid or duplicate URI")
        seen_uris.add(uri)


def _relative_collection_uri(uri, root: str) -> str:
    if not isinstance(uri, str) or not isinstance(root, str):
        raise ValueError("collection index URI is invalid")
    root = root.rstrip("/")
    prefix = f"{root}/"
    if not uri.startswith(prefix):
        raise ValueError("collection index URI is outside its declared asset root")
    relative = uri[len(prefix):]
    if not relative or not _safe_relative_path(relative, allow_trailing=True):
        raise ValueError("collection index URI is invalid")
    return relative


def _safe_relative_path(path, *, allow_trailing: bool = False) -> bool:
    if not isinstance(path, str) or not path or path.startswith("/"):
        return False
    if any(char in path for char in ("\\", "\r", "\n")):
        return False
    value = path[:-1] if allow_trailing and path.endswith("/") else path
    if not value:
        return False
    parts = value.split("/")
    return all(part not in {"", ".", ".."} for part in parts)


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(char in "0123456789abcdef" for char in value)


def _nonnegative_integer(value, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} is invalid")
    try:
        numeric = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field} is invalid") from exc
    if numeric < 0 or numeric != value:
        raise ValueError(f"{field} is invalid")
    return numeric


def _positive_integer(value, field: str) -> int:
    numeric = _nonnegative_integer(value, field)
    if numeric < 1:
        raise ValueError(f"{field} is invalid")
    return numeric


def _sample_rows(rows: list[dict], fraction: float,
                 sampling_key: bytes) -> list[dict]:
    if fraction >= 1 or not rows:
        return rows
    count = max(1, int(math.ceil(len(rows) * fraction)))
    return sorted(
        rows,
        key=lambda row: hashlib.sha256(
            sampling_key + b"\x00" +
            str(row.get("sample_id", "")).encode("utf-8")
        ).hexdigest(),
    )[:count]


def _represented_keys(s3, configured_bucket: str, row: dict) -> set[str]:
    try:
        uri_bucket, key = parse_s3_uri(row.get("uri") or "")
    except ValueError:
        return set()
    if row.get("source_kind") == "dicom_series" or key.endswith("/"):
        bucket = uri_bucket or configured_bucket
        prefix = key if key.endswith("/") else f"{key}/"
        return {
            obj["key"] for obj in list_objects(s3, bucket, prefix)
            if obj["key"].lower().endswith(".dcm")
        }
    return {key}


def _verify_row(s3, configured_bucket: str, row: dict,
                manifest_rows: dict[str, dict], quick: bool) -> tuple[VerifyIssue | None, set[str], bool]:
    uri = row.get("uri") or ""
    sample_id = row.get("sample_id") or ""
    expected_hash = row.get("content_hash") or ""
    expected_size = int(row.get("size") or 0)
    expected_version = row.get("version_id")
    hash_version = row.get("content_hash_version_id")

    try:
        uri_bucket, key = parse_s3_uri(uri)
    except ValueError as e:
        return VerifyIssue(sample_id, uri, "mismatch", str(e)), set(), False
    bucket = uri_bucket or configured_bucket

    source_kind = row.get("source_kind")
    if source_kind == "dicom_series" or key.endswith("/"):
        return _verify_dicom_series(
            s3, bucket, key, sample_id, uri, expected_hash,
            expected_size, quick,
            expected_n_files=(manifest_rows.get(sample_id) or {}).get("n_files"),
        )

    meta = head_object(s3, bucket, key)
    if not meta:
        return VerifyIssue(sample_id, uri, "missing", "object is absent"), {key}, False
    if meta.get("size") != expected_size:
        return VerifyIssue(
            sample_id, uri, "mismatch",
            f"size {meta.get('size')} != recorded {expected_size}",
        ), {key}, False
    # A matching immutable version id is authoritative. ETags are not: for
    # common S3 uploads they are only MD5-derived and must never replace the
    # recorded SHA-256 content check.
    if (quick and expected_version not in (None, "", "null") and
            hash_version == expected_version and
            meta.get("version_id") == expected_version and
            meta.get("size") == expected_size):
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
                         quick: bool,
                         expected_n_files=None,
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
    if total_size != expected_size:
        return VerifyIssue(
            sample_id, uri, "mismatch",
            f"series size {total_size} != recorded {expected_size}",
        ), keys, False
    if expected_n_files is not None and len(objects) != int(expected_n_files):
        return VerifyIssue(
            sample_id, uri, "mismatch",
            f"series file count {len(objects)} != recorded {expected_n_files}",
        ), keys, False
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
