"""Manifest and index generation."""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from copy import deepcopy

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import yaml

from .hashing import sha256_file, is_image_file, sample_id_from_filename

DATASET_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
ID_COL = "sample_id"
PRIVACY_UNIT = "patient"
PRIVACY_UNIT_CANONICALIZATION = "trim-utf8-v2"
_ASCII_ID_TRIM = " \t\r\n"
_MAX_PRIVACY_UNIT_BYTES = 4096
_MISSING_VALUES = {"na", "nan", "null", "<na>", "nat"}


def validate_dataset_id(dataset_id: str) -> str:
    """Validate and return a dsimaging dataset identifier."""
    if not DATASET_ID_RE.fullmatch(dataset_id or ""):
        raise ValueError(
            "dataset_id must match ^[a-z0-9][a-z0-9._-]*$ "
            "(lowercase letters, digits, dot, underscore and dash)"
        )
    return dataset_id


def scan_images(source_dir: str) -> list[dict]:
    """Scan a local directory for image files and compute hashes.

    Returns a list of sample dicts with:
        sample_id, source_kind, primary_filename, files, content_hash, size, local_path
    """
    samples = []
    images_dir = _find_images_dir(source_dir)
    if not images_dir:
        return samples

    for entry in sorted(os.listdir(images_dir)):
        filepath = os.path.join(images_dir, entry)

        if os.path.isfile(filepath) and is_image_file(entry):
            samples.append({
                "sample_id": sample_id_from_filename(entry),
                "source_kind": "single_file",
                "primary_filename": entry,
                "uri_path": entry,
                "files": [{"path": entry, "role": "primary"}],
                "content_hash": sha256_file(filepath),
                "size": os.path.getsize(filepath),
                "local_path": filepath,
            })
        elif os.path.isdir(filepath):
            dcm_files = sorted(
                f for f in os.listdir(filepath) if f.lower().endswith(".dcm")
            )
            if dcm_files:
                h = hashlib.sha256()
                total_size = 0
                for dcm in dcm_files:
                    dcm_path = os.path.join(filepath, dcm)
                    h.update(sha256_file(dcm_path).encode())
                    total_size += os.path.getsize(dcm_path)
                samples.append({
                    "sample_id": entry,
                    "source_kind": "dicom_series",
                    "primary_filename": None,
                    "uri_path": f"{entry}/",
                    "files": [{"path": f"{entry}/{f}", "role": "slice"} for f in dcm_files],
                    "content_hash": h.hexdigest(),
                    "size": total_size,
                    "local_path": filepath,
                })

    _validate_sample_ids(samples)
    return samples


def validate_dicom_series(source_dir: str) -> list[str]:
    """Return warnings for basic DICOM series consistency checks.

    The check is intentionally lightweight and metadata-only. If pydicom is not
    installed, callers get a warning instead of a hard dependency failure.
    """
    images_dir = _find_images_dir(source_dir)
    if not images_dir:
        return []
    try:
        import pydicom
    except ImportError:
        return ["pydicom is not installed; DICOM sanity checks were skipped"]

    warnings = []
    for entry in sorted(os.listdir(images_dir)):
        series_dir = os.path.join(images_dir, entry)
        if not os.path.isdir(series_dir):
            continue
        dcm_files = sorted(
            f for f in os.listdir(series_dir) if f.lower().endswith(".dcm")
        )
        if not dcm_files:
            continue
        series_uids = set()
        modalities = set()
        instance_numbers = []
        unreadable = []
        for filename in dcm_files:
            path = os.path.join(series_dir, filename)
            try:
                ds = pydicom.dcmread(path, stop_before_pixels=True, force=True)
            except Exception as e:
                unreadable.append(f"{filename}: {e}")
                continue
            if getattr(ds, "SeriesInstanceUID", None):
                series_uids.add(str(ds.SeriesInstanceUID))
            if getattr(ds, "Modality", None):
                modalities.add(str(ds.Modality))
            if getattr(ds, "InstanceNumber", None) is not None:
                try:
                    instance_numbers.append(int(ds.InstanceNumber))
                except (TypeError, ValueError):
                    warnings.append(
                        f"{entry}: non-integer InstanceNumber in {filename}"
                    )
        if unreadable:
            warnings.append(f"{entry}: unreadable DICOM metadata in {len(unreadable)} file(s)")
        if len(series_uids) > 1:
            warnings.append(f"{entry}: multiple SeriesInstanceUID values")
        if len(modalities) > 1:
            warnings.append(f"{entry}: multiple Modality values")
        if instance_numbers:
            if len(instance_numbers) != len(set(instance_numbers)):
                warnings.append(f"{entry}: duplicate InstanceNumber values")
            if instance_numbers != sorted(instance_numbers):
                warnings.append(f"{entry}: InstanceNumber is not monotonic by filename")
    return warnings


def scan_masks(source_dir: str, sample_ids: list[str] | None = None) -> list[dict]:
    """Scan a local directory for mask files and compute hashes."""
    masks = []
    masks_dir = _find_masks_dir(source_dir)
    if not masks_dir:
        return masks

    for root, _, files in os.walk(masks_dir):
        for entry in sorted(files):
            filepath = os.path.join(root, entry)
            if not os.path.isfile(filepath) or not is_image_file(entry):
                continue
            rel = os.path.relpath(filepath, masks_dir).replace(os.sep, "/")
            masks.append({
                "sample_id": _sample_id_from_mask_filename(entry, sample_ids),
                "source_kind": "mask_file",
                "primary_filename": entry,
                "uri_path": rel,
                "files": [{"path": rel, "role": "mask"}],
                "content_hash": sha256_file(filepath),
                "size": os.path.getsize(filepath),
                "local_path": filepath,
            })

    masks = sorted(masks, key=lambda sample: sample["sample_id"])
    _validate_mask_ids(masks, sample_ids)
    return masks


def scan_s3_images(s3, bucket: str, prefix: str, objects: list[dict]) -> list[dict]:
    """Build sample records from S3 objects under ``<prefix>/source/images``.

    Hashes are computed by streaming objects through temporary files. Single-file
    samples and one-directory DICOM series use the same sample model as
    ``scan_images()`` so publish, rescan and the store controller converge on
    identical parquet schemas.
    """
    root = f"{prefix.rstrip('/')}/source/images/"
    single_files = []
    dicom_groups = {}

    for obj in objects:
        key = obj["key"]
        if not key.startswith(root):
            continue
        rel = key[len(root):]
        if not rel or rel.endswith("/"):
            continue
        filename = rel.rsplit("/", 1)[-1]
        if not is_image_file(filename):
            continue
        if "/" in rel and filename.lower().endswith(".dcm"):
            sample_id = rel.split("/", 1)[0]
            dicom_groups.setdefault(sample_id, []).append((rel, obj))
        else:
            single_files.append((rel, obj))

    samples = []
    for rel, obj in sorted(single_files, key=lambda item: item[0]):
        content_hash = _sha256_s3_object(s3, bucket, obj["key"])
        filename = rel.rsplit("/", 1)[-1]
        samples.append({
            "sample_id": sample_id_from_filename(filename),
            "source_kind": "single_file",
            "primary_filename": filename,
            "uri_path": rel,
            "files": [{"path": rel, "role": "primary"}],
            "content_hash": content_hash,
            "size": int(obj.get("size", 0)),
            "last_modified": obj.get("last_modified"),
            "version_id": obj.get("version_id"),
            "etag": obj.get("etag"),
        })

    for sample_id in sorted(dicom_groups):
        h = hashlib.sha256()
        total_size = 0
        files = []
        last_modified = None
        etags = []
        for rel, obj in sorted(dicom_groups[sample_id], key=lambda item: item[0]):
            content_hash = _sha256_s3_object(s3, bucket, obj["key"])
            h.update(content_hash.encode())
            total_size += int(obj.get("size", 0))
            last_modified = obj.get("last_modified") or last_modified
            if obj.get("etag"):
                etags.append(obj["etag"])
            files.append({"path": rel, "role": "slice"})
        samples.append({
            "sample_id": sample_id,
            "source_kind": "dicom_series",
            "primary_filename": None,
            "uri_path": f"{sample_id}/",
            "files": files,
            "content_hash": h.hexdigest(),
            "size": total_size,
            "last_modified": last_modified,
            "version_id": None,
            "etag": ",".join(etags) if etags else None,
        })

    samples = sorted(samples, key=lambda sample: sample["sample_id"])
    _validate_sample_ids(samples)
    return samples


def scan_s3_masks(s3, bucket: str, prefix: str, objects: list[dict],
                  sample_ids: list[str] | None = None) -> list[dict]:
    """Build mask records from S3 objects under ``<prefix>/source/masks``."""
    root = f"{prefix.rstrip('/')}/source/masks/"
    masks = []

    for obj in objects:
        key = obj["key"]
        if not key.startswith(root):
            continue
        rel = key[len(root):]
        if not rel or rel.endswith("/"):
            continue
        filename = rel.rsplit("/", 1)[-1]
        if not is_image_file(filename):
            continue
        content_hash = _sha256_s3_object(s3, bucket, key)
        masks.append({
            "sample_id": _sample_id_from_mask_filename(filename, sample_ids),
            "source_kind": "mask_file",
            "primary_filename": filename,
            "uri_path": rel,
            "files": [{"path": rel, "role": "mask"}],
            "content_hash": content_hash,
            "size": int(obj.get("size", 0)),
            "last_modified": obj.get("last_modified"),
            "version_id": obj.get("version_id"),
            "etag": obj.get("etag"),
        })

    masks = sorted(masks, key=lambda sample: sample["sample_id"])
    _validate_mask_ids(masks, sample_ids)
    return masks


def generate_manifest(dataset_id: str, bucket: str, prefix: str,
                      modality: str = "unknown", has_masks: bool = False,
                      mask_asset: str = "masks", *,
                      privacy_unit_col: str,
                      label_col: str | None = None,
                      existing_manifest: dict | None = None) -> dict:
    """Generate a manifest dict for a dataset."""
    validate_dataset_id(dataset_id)
    contract = metadata_contract(privacy_unit_col, label_col)
    manifest = deepcopy(existing_manifest) if existing_manifest is not None else {}
    manifest.update({
        "schema_version": manifest.get("schema_version", 1),
        "dataset_id": dataset_id,
        "modality": modality,
        "content_hash_index": {
            "uri": f"s3://{bucket}/{prefix}/indexes/content_hash_index.parquet",
            "format": "parquet",
        },
        "sample_manifests": {
            "uri": f"s3://{bucket}/{prefix}/metadata/sample_manifests.parquet",
            "format": "parquet",
        },
    })
    assets = deepcopy(manifest.get("assets")) if isinstance(manifest.get("assets"), dict) else {}
    assets["images"] = {
        "uri": f"s3://{bucket}/{prefix}/source/images/",
        "kind": "image_root",
    }
    metadata = deepcopy(manifest.get("metadata")) if isinstance(manifest.get("metadata"), dict) else {}
    metadata.update({
        "uri": f"s3://{bucket}/{prefix}/metadata/samples.parquet",
        "format": "parquet",
        **contract,
    })
    if label_col is None:
        metadata.pop("label_col", None)
    manifest["assets"] = assets
    manifest["metadata"] = metadata
    if has_masks:
        assets[mask_asset] = {
            "uri": f"s3://{bucket}/{prefix}/source/masks/",
            "kind": "mask_root",
            "content_hash_index": (
                f"s3://{bucket}/{prefix}/indexes/"
                f"{mask_asset}_content_hash_index.parquet"
            ),
        }
    else:
        assets.pop(mask_asset, None)
    return manifest


def metadata_contract(privacy_unit_col: str,
                      label_col: str | None = None) -> dict:
    """Return the pinned disclosure-control contract for sample metadata."""
    privacy_unit_col = _validate_column_name(privacy_unit_col, "privacy_unit_col")
    if (privacy_unit_col == ID_COL or
            privacy_unit_col.lower() in {"source_kind", "n_files"}):
        raise ValueError(
            "privacy_unit_col must identify a dedicated patient column"
        )
    contract = {
        "id_col": ID_COL,
        "privacy_unit": PRIVACY_UNIT,
        "privacy_unit_col": privacy_unit_col,
        "privacy_unit_canonicalization": PRIVACY_UNIT_CANONICALIZATION,
    }
    if label_col is not None:
        label_col = _validate_column_name(label_col, "label_col")
        if label_col in {ID_COL, "source_kind", "n_files", privacy_unit_col}:
            raise ValueError(
                "label_col must be distinct from sample and patient columns"
            )
        contract["label_col"] = label_col
    return contract


def metadata_contract_from_manifest(manifest: dict) -> dict:
    """Read and validate a manifest's pinned metadata privacy contract."""
    if not isinstance(manifest, dict):
        raise ValueError("manifest must be a mapping")
    metadata = manifest.get("metadata")
    if not isinstance(metadata, dict):
        raise ValueError("manifest metadata must be a mapping")
    if metadata.get("id_col") != ID_COL:
        raise ValueError("manifest metadata.id_col must be 'sample_id'")
    if metadata.get("privacy_unit") != PRIVACY_UNIT:
        raise ValueError("manifest metadata.privacy_unit must be 'patient'")
    if metadata.get("privacy_unit_canonicalization") != PRIVACY_UNIT_CANONICALIZATION:
        raise ValueError(
            "manifest metadata.privacy_unit_canonicalization must be 'trim-utf8-v2'"
        )
    return metadata_contract(
        metadata.get("privacy_unit_col"), metadata.get("label_col")
    )


def write_manifest_yaml(manifest: dict, path: str):
    with open(path, "w") as f:
        yaml.dump(manifest, f, default_flow_style=False, sort_keys=False)


def build_hash_index(samples: list[dict], bucket: str, prefix: str,
                     source_path: str = "images") -> pa.Table:
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ")
    if not samples:
        return pa.table({
            "sample_id": pa.array([], type=pa.string()),
            "uri": pa.array([], type=pa.string()),
            "content_hash": pa.array([], type=pa.string()),
            "size": pa.array([], type=pa.int64()),
            "last_modified": pa.array([], type=pa.string()),
            "version_id": pa.array([], type=pa.string()),
            "etag": pa.array([], type=pa.string()),
            "source_kind": pa.array([], type=pa.string()),
        })
    return pa.table({
        "sample_id": [s["sample_id"] for s in samples],
        "uri": [
            f"s3://{bucket}/{prefix}/source/{source_path}/{s.get('uri_path') or s['primary_filename']}"
            if s.get("uri_path") or s["primary_filename"]
            else f"s3://{bucket}/{prefix}/source/{source_path}/{s['sample_id']}/"
            for s in samples
        ],
        "content_hash": [s["content_hash"] for s in samples],
        "size": pa.array([s["size"] for s in samples], type=pa.int64()),
        "last_modified": [s.get("last_modified") or now for s in samples],
        "version_id": pa.array([s.get("version_id") for s in samples], type=pa.string()),
        "etag": pa.array([s.get("etag") for s in samples], type=pa.string()),
        "source_kind": [s["source_kind"] for s in samples],
    })


def build_mask_hash_index(masks: list[dict], bucket: str, prefix: str) -> pa.Table:
    return build_hash_index(masks, bucket, prefix, source_path="masks")


def build_sample_manifests(samples: list[dict]) -> pa.Table:
    if not samples:
        return pa.table({
            "sample_id": pa.array([], type=pa.string()),
            "source_kind": pa.array([], type=pa.string()),
            "primary_uri": pa.array([], type=pa.string()),
            "files_json": pa.array([], type=pa.string()),
            "content_hash": pa.array([], type=pa.string()),
            "n_files": pa.array([], type=pa.int32()),
        })
    return pa.table({
        "sample_id": [s["sample_id"] for s in samples],
        "source_kind": [s["source_kind"] for s in samples],
        "primary_uri": pa.array(
            [
                (s.get("uri_path") or s["primary_filename"])
                if s["source_kind"] == "single_file"
                else s["primary_filename"]
                for s in samples
            ],
            type=pa.string(),
        ),
        "files_json": [json.dumps(s["files"]) for s in samples],
        "content_hash": [s["content_hash"] for s in samples],
        "n_files": pa.array([len(s["files"]) for s in samples], type=pa.int32()),
    })


def build_samples_metadata(samples: list[dict],
                           extra_metadata: pa.Table | None = None, *,
                           privacy_unit_col: str | None = None,
                           label_col: str | None = None) -> pa.Table:
    """Build sample metadata from an exact image/metadata sample roster."""
    _validate_sample_ids(samples)
    if not samples:
        base = pa.table({
            "sample_id": pa.array([], type=pa.string()),
            "source_kind": pa.array([], type=pa.string()),
            "n_files": pa.array([], type=pa.int32()),
        })
        if extra_metadata is None:
            result = base
        else:
            result = _left_join_metadata(base, extra_metadata)
        return validate_samples_metadata(
            result, privacy_unit_col, label_col
        ) if privacy_unit_col is not None else result
    base = pa.table({
        "sample_id": [s["sample_id"] for s in samples],
        "source_kind": [s["source_kind"] for s in samples],
        "n_files": pa.array([len(s["files"]) for s in samples], type=pa.int32()),
    })
    result = base if extra_metadata is None else _left_join_metadata(base, extra_metadata)
    return validate_samples_metadata(
        result, privacy_unit_col, label_col
    ) if privacy_unit_col is not None else result


def validate_samples_metadata(table: pa.Table, privacy_unit_col: str,
                              label_col: str | None = None) -> pa.Table:
    """Fail closed unless every sample has its pinned patient unit and label."""
    contract = metadata_contract(privacy_unit_col, label_col)
    table = _normalise_metadata_table(table)
    ids = table[ID_COL].to_pylist()
    _validate_values(ids, ID_COL, canonical_patient_ids=False)
    _require_canonical_sample_ids(ids)
    duplicates = _duplicates(ids)
    if duplicates:
        raise ValueError(f"metadata has duplicate sample_id: {duplicates[0]}")
    for key in ("privacy_unit_col", "label_col"):
        column = contract.get(key)
        if column is None:
            continue
        if column not in table.column_names:
            raise ValueError(f"metadata must contain declared {key} column: {column}")
        _validate_values(
            table[column].to_pylist(), column,
            canonical_patient_ids=(key == "privacy_unit_col"),
        )
        if key == "privacy_unit_col":
            values = [_canonical_identifier(value)
                      for value in table[column].to_pylist()]
            index = table.column_names.index(column)
            table = table.set_column(
                index, column, pa.array(values, type=pa.string()))
    return table


def read_metadata_table(path: str) -> pa.Table:
    """Read a CSV or Parquet metadata table with a string ``sample_id``."""
    lower = path.lower()
    if lower.endswith(".parquet"):
        table = pq.read_table(path)
    elif lower.endswith(".csv"):
        table = pacsv.read_csv(path)
    else:
        raise ValueError("metadata must be a .csv or .parquet file")
    return _normalise_metadata_table(table)


def _normalise_metadata_table(table: pa.Table) -> pa.Table:
    if "sample_id" not in table.column_names:
        raise ValueError("metadata must contain a sample_id column")
    idx = table.column_names.index("sample_id")
    return table.set_column(idx, "sample_id", table["sample_id"].cast(pa.string()))


def _validate_column_name(value, field: str) -> str:
    if not isinstance(value, str) or not value.strip(_ASCII_ID_TRIM):
        raise ValueError(f"{field} must be a non-empty column name")
    return value.strip(_ASCII_ID_TRIM)


def _canonical_identifier(value) -> str:
    """Apply the shared trim-utf8-v2 identifier contract."""
    return str(value).strip(_ASCII_ID_TRIM)


def _require_canonical_sample_ids(values: list) -> None:
    """Reject sample IDs whose canonical form would change asset linkage."""
    for row_number, value in enumerate(values, start=1):
        canonical = _canonical_identifier(value)
        if str(value) != canonical:
            raise ValueError(
                "metadata sample_id must already satisfy trim-utf8-v2 "
                f"at row {row_number}"
            )
        if len(canonical.encode("utf-8", errors="strict")) > _MAX_PRIVACY_UNIT_BYTES:
            raise ValueError(
                f"metadata sample_id exceeds 4096 UTF-8 bytes at row {row_number}"
            )


def _validate_values(values: list, column: str, *,
                     canonical_patient_ids: bool) -> None:
    for row_number, value in enumerate(values, start=1):
        if value is None:
            raise ValueError(f"metadata column {column} is empty at row {row_number}")
        try:
            text = str(value).strip(_ASCII_ID_TRIM)
            encoded = text.encode("utf-8", errors="strict")
        except (TypeError, UnicodeError, ValueError) as exc:
            raise ValueError(
                f"metadata column {column} is invalid at row {row_number}"
            ) from exc
        if not text or text.lower() in _MISSING_VALUES:
            raise ValueError(f"metadata column {column} is empty at row {row_number}")
        if canonical_patient_ids and len(encoded) > _MAX_PRIVACY_UNIT_BYTES:
            raise ValueError(
                f"metadata column {column} exceeds 4096 UTF-8 bytes at row {row_number}"
            )


def _validate_sample_ids(samples: list[dict]) -> None:
    ids = [sample.get(ID_COL) for sample in samples]
    _validate_values(ids, ID_COL, canonical_patient_ids=False)
    _require_canonical_sample_ids(ids)
    duplicates = _duplicates(ids)
    if duplicates:
        raise ValueError(f"duplicate sample_id discovered: {duplicates[0]}")


def _validate_mask_ids(masks: list[dict], sample_ids: list[str] | None) -> None:
    """Require a one-to-zero/one mapping from admitted samples to masks."""
    _validate_sample_ids(masks)
    if sample_ids is None:
        return
    allowed = set(sample_ids)
    orphan = next(
        (mask[ID_COL] for mask in masks if mask[ID_COL] not in allowed), None)
    if orphan is not None:
        raise ValueError(f"mask has no matching image sample_id: {orphan}")


def _duplicates(values: list) -> list:
    seen = set()
    duplicates = []
    for value in values:
        if value in seen and value not in duplicates:
            duplicates.append(value)
        seen.add(value)
    return duplicates


def _left_join_metadata(base: pa.Table, extra_metadata: pa.Table) -> pa.Table:
    extra_metadata = _normalise_metadata_table(extra_metadata)
    _validate_values(
        extra_metadata[ID_COL].to_pylist(), ID_COL,
        canonical_patient_ids=False)
    _require_canonical_sample_ids(extra_metadata[ID_COL].to_pylist())
    base_ids = base["sample_id"].to_pylist()
    rows_by_id: dict[str, dict] = {}
    for row in extra_metadata.to_pylist():
        sample_id = row.get("sample_id")
        if sample_id in rows_by_id:
            raise ValueError(f"metadata has duplicate sample_id: {sample_id}")
        rows_by_id[sample_id] = row

    base_set = set(base_ids)
    metadata_set = set(rows_by_id)
    if base_set != metadata_set:
        raise ValueError(
            "metadata sample_id roster must exactly match discovered images"
        )

    extra_columns = [
        name for name in extra_metadata.column_names
        if name != "sample_id" and name not in base.column_names
    ]
    arrays = {name: base[name] for name in base.column_names}
    schema = extra_metadata.schema
    for name in extra_columns:
        field = schema.field(name)
        values = [
            rows_by_id.get(sample_id, {}).get(name)
            for sample_id in base_ids
        ]
        arrays[name] = pa.array(values, type=field.type)

    return pa.table(arrays)


def _find_images_dir(source_dir: str) -> str | None:
    """Find the directory containing image files."""
    for candidate in ["images", "source/images", "."]:
        d = os.path.join(source_dir, candidate)
        if os.path.isdir(d) and _contains_supported_images(d):
            return d
    return None


def _find_masks_dir(source_dir: str) -> str | None:
    """Find the directory containing mask files."""
    for candidate in ["masks", "source/masks", "labels", "source/labels"]:
        d = os.path.join(source_dir, candidate)
        if os.path.isdir(d) and _contains_supported_masks(d):
            return d
    return None


def _contains_supported_images(directory: str) -> bool:
    for entry in os.listdir(directory):
        path = os.path.join(directory, entry)
        if os.path.isfile(path) and is_image_file(entry):
            return True
        if os.path.isdir(path):
            if any(f.lower().endswith(".dcm") for f in os.listdir(path)):
                return True
    return False


def _contains_supported_masks(directory: str) -> bool:
    for _, _, files in os.walk(directory):
        if any(is_image_file(f) for f in files):
            return True
    return False


def _sample_id_from_mask_filename(filename: str,
                                  sample_ids: list[str] | None = None) -> str:
    stem = sample_id_from_filename(filename)
    if sample_ids:
        for sample_id in sorted(sample_ids, key=len, reverse=True):
            if stem == sample_id or stem.startswith(f"{sample_id}_") or stem.startswith(f"{sample_id}-"):
                return sample_id
    suffix_pattern = (
        r"(?i)(?:[_-](?:mask|seg|label|labels|roi|gtv[-_]?\d*|"
        r"lesion[-_]?\d*|tumou?r[-_]?\d*))$"
    )
    stripped = re.sub(suffix_pattern, "", stem)
    return stripped or stem


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
