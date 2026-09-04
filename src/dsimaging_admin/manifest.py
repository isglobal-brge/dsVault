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
_MAX_MEDICAL_HEADER_BYTES = 1024 * 1024
_MISSING_VALUES = {"na", "nan", "null", "<na>", "nat"}
_RESOURCE_FIELDS = (
    "uri", "file", "path", "root", "manifest",
    "content_hash_index", "hash_index", "index_uri",
)


def validate_dataset_id(dataset_id: str) -> str:
    """Validate and return a dsimaging dataset identifier."""
    if (not isinstance(dataset_id, str) or
            len(dataset_id.encode("utf-8")) > 128 or
            not DATASET_ID_RE.fullmatch(dataset_id) or
            ".." in dataset_id):
        raise ValueError(
            "dataset_id must match ^[a-z0-9][a-z0-9._-]*$, be at most "
            "128 bytes, and not contain consecutive dots"
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
        if os.path.islink(filepath):
            raise ValueError("symbolic links are not allowed in image collections")

        if os.path.isfile(filepath) and is_image_file(entry):
            _validate_relative_asset_path(entry)
            _validate_local_image_container(filepath, entry)
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
                _validate_relative_asset_path(entry)
                h = hashlib.sha256()
                total_size = 0
                for dcm in dcm_files:
                    dcm_path = os.path.join(filepath, dcm)
                    if os.path.islink(dcm_path):
                        raise ValueError(
                            "symbolic links are not allowed in DICOM series")
                    _validate_relative_asset_path(f"{entry}/{dcm}")
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

    for root, directories, files in os.walk(masks_dir):
        if any(os.path.islink(os.path.join(root, name)) for name in directories):
            raise ValueError("symbolic links are not allowed in mask collections")
        for entry in sorted(files):
            filepath = os.path.join(root, entry)
            if not os.path.isfile(filepath) or not is_image_file(entry):
                continue
            if os.path.islink(filepath):
                raise ValueError("symbolic links are not allowed in mask collections")
            rel = os.path.relpath(filepath, masks_dir).replace(os.sep, "/")
            _validate_relative_asset_path(rel)
            _validate_local_image_container(filepath, entry)
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
        _validate_relative_asset_path(rel)
        if "/" in rel and filename.lower().endswith(".dcm"):
            sample_id = rel.split("/", 1)[0]
            dicom_groups.setdefault(sample_id, []).append((rel, obj))
        else:
            single_files.append((rel, obj))

    samples = []
    for rel, obj in sorted(single_files, key=lambda item: item[0]):
        filename = rel.rsplit("/", 1)[-1]
        content_hash = _sha256_s3_object(
            s3, bucket, obj["key"], obj.get("version_id"),
            container_name=filename)
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
            "content_hash_version_id": (
                obj.get("version_id")
                if obj.get("version_id") not in (None, "", "null")
                else None
            ),
            "etag": obj.get("etag"),
        })

    for sample_id in sorted(dicom_groups):
        h = hashlib.sha256()
        total_size = 0
        files = []
        last_modified = None
        etags = []
        for rel, obj in sorted(dicom_groups[sample_id], key=lambda item: item[0]):
            content_hash = _sha256_s3_object(
                s3, bucket, obj["key"], obj.get("version_id"))
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
        _validate_relative_asset_path(rel)
        content_hash = _sha256_s3_object(
            s3, bucket, key, obj.get("version_id"),
            container_name=filename)
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
            "content_hash_version_id": (
                obj.get("version_id")
                if obj.get("version_id") not in (None, "", "null")
                else None
            ),
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
                      label_levels: list[str] | tuple[str, ...] | None = None,
                      existing_manifest: dict | None = None) -> dict:
    """Generate a manifest dict for a dataset."""
    validate_dataset_id(dataset_id)
    contract = metadata_contract(privacy_unit_col, label_col, label_levels)
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
    metadata = {
        "uri": f"s3://{bucket}/{prefix}/metadata/samples.parquet",
        "format": "parquet",
        **contract,
    }
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
    validate_manifest_scope(manifest, bucket, prefix)
    return manifest


def metadata_contract(privacy_unit_col: str,
                      label_col: str | None = None,
                      label_levels: list[str] | tuple[str, ...] | None = None) -> dict:
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
    public_levels = _validate_public_label_levels(label_levels, label_col)
    if public_levels:
        contract["label_levels"] = public_levels
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
        metadata.get("privacy_unit_col"), metadata.get("label_col"),
        metadata.get("label_levels"),
    )


def validate_manifest_scope(manifest: dict, bucket: str, prefix: str) -> None:
    """Require every store manifest reference to stay in one collection."""
    if not isinstance(manifest, dict):
        raise ValueError("manifest must be a mapping")
    dataset_id = validate_dataset_id(manifest.get("dataset_id"))
    prefix = (prefix or "").rstrip("/")
    if prefix != f"datasets/{dataset_id}":
        raise ValueError("manifest dataset_id does not match its collection prefix")
    if not isinstance(bucket, str) or not bucket:
        raise ValueError("manifest bucket must be non-empty")
    if (not isinstance(manifest.get("schema_version"), int) or
            isinstance(manifest.get("schema_version"), bool) or
            manifest.get("schema_version") != 1):
        raise ValueError("manifest schema_version must be 1")

    root = f"s3://{bucket}/{prefix}"
    expected = {
        "content_hash_index": (
            f"{root}/indexes/content_hash_index.parquet", "parquet"),
        "sample_manifests": (
            f"{root}/metadata/sample_manifests.parquet", "parquet"),
        "metadata": (f"{root}/metadata/samples.parquet", "parquet"),
    }
    for field, (uri, file_format) in expected.items():
        value = manifest.get(field)
        if (not isinstance(value, dict) or value.get("uri") != uri or
                value.get("format") != file_format):
            raise ValueError(f"manifest {field} does not use its canonical collection URI")
        _reject_conflicting_resource_aliases(value, uri, field)

    assets = manifest.get("assets")
    images = assets.get("images") if isinstance(assets, dict) else None
    if (not isinstance(images, dict) or
            images.get("uri") != f"{root}/source/images/" or
            images.get("kind") != "image_root"):
        raise ValueError("manifest images asset is not the canonical collection root")
    _reject_conflicting_resource_aliases(
        images, f"{root}/source/images/", "images asset")
    masks = assets.get("masks")
    if masks is not None and (
            not isinstance(masks, dict) or
            masks.get("uri") != f"{root}/source/masks/" or
            masks.get("kind") != "mask_root" or
            masks.get("content_hash_index") !=
            f"{root}/indexes/masks_content_hash_index.parquet"):
        raise ValueError("manifest masks asset is not the canonical collection root")
    if masks is not None:
        _reject_conflicting_resource_aliases(
            masks, f"{root}/source/masks/", "masks asset")

    metadata_contract_from_manifest(manifest)
    references = []
    for value in (manifest.get("metadata"), manifest.get("content_hash_index"),
                  manifest.get("sample_manifests")):
        references.extend(_manifest_resource_references(value))
    for asset in assets.values():
        if not isinstance(asset, dict):
            raise ValueError("manifest assets must be mappings")
        references.extend(_manifest_resource_references(asset))
    labels = manifest.get("labels") or []
    if isinstance(labels, dict):
        labels = labels.values()
    elif not isinstance(labels, list):
        raise ValueError("manifest labels must be a list or mapping")
    for label in labels:
        if not isinstance(label, dict):
            raise ValueError("manifest labels must contain mappings")
        references.extend(_manifest_resource_references(label))
    for uri in references:
        if not _uri_within_collection(uri, root):
            raise ValueError("manifest references data outside its collection prefix")


def _manifest_resource_references(value) -> list[str]:
    if not isinstance(value, dict):
        return []
    references = []
    for field in _RESOURCE_FIELDS:
        candidate = value.get(field)
        if candidate is None:
            continue
        if isinstance(candidate, dict):
            candidate = candidate.get("uri")
        if not isinstance(candidate, str) or not candidate:
            raise ValueError(f"manifest resource field {field} must be a URI")
        references.append(candidate)
    return references


def _reject_conflicting_resource_aliases(value: dict, uri: str,
                                         field: str) -> None:
    for alias in ("file", "path", "root", "manifest"):
        candidate = value.get(alias)
        if candidate is not None and candidate != uri:
            raise ValueError(f"manifest {field} has a conflicting resource alias")


def _uri_within_collection(uri: str, root: str) -> bool:
    if any(char in uri for char in ("\\", "\r", "\n")):
        return False
    if uri != root and not uri.startswith(f"{root}/"):
        return False
    return not any(part in {".", ".."} for part in uri.split("/"))


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
            "content_hash_version_id": pa.array([], type=pa.string()),
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
        "content_hash_version_id": pa.array(
            [s.get("content_hash_version_id") for s in samples],
            type=pa.string(),
        ),
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
                           label_col: str | None = None,
                           label_levels: list[str] | tuple[str, ...] | None = None) -> pa.Table:
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
            result, privacy_unit_col, label_col, label_levels
        ) if privacy_unit_col is not None else result
    base = pa.table({
        "sample_id": [s["sample_id"] for s in samples],
        "source_kind": [s["source_kind"] for s in samples],
        "n_files": pa.array([len(s["files"]) for s in samples], type=pa.int32()),
    })
    result = base if extra_metadata is None else _left_join_metadata(base, extra_metadata)
    return validate_samples_metadata(
        result, privacy_unit_col, label_col, label_levels
    ) if privacy_unit_col is not None else result


def validate_samples_metadata(table: pa.Table, privacy_unit_col: str,
                              label_col: str | None = None,
                              label_levels: list[str] | tuple[str, ...] | None = None) -> pa.Table:
    """Fail closed unless every sample has its pinned patient unit and label."""
    contract = metadata_contract(privacy_unit_col, label_col, label_levels)
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
    public_levels = contract.get("label_levels")
    if public_levels:
        labels = [str(value) for value in table[contract["label_col"]].to_pylist()]
        if not set(labels).issubset(set(public_levels)):
            raise ValueError(
                "metadata labels must belong to the declared public label vocabulary"
            )
        identifiers = set(ids)
        identifiers.update(table[contract["privacy_unit_col"]].to_pylist())
        if identifiers.intersection(public_levels):
            raise ValueError(
                "metadata labels must not equal sample or patient identifiers"
            )
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


def _validate_public_label_levels(
        values: list[str] | tuple[str, ...] | None,
        label_col: str | None) -> list[str]:
    if values is None:
        return []
    if isinstance(values, (str, bytes)) or not isinstance(values, (list, tuple)):
        raise ValueError("label_levels must be a sequence of public labels")
    levels = list(values)
    if not levels:
        return []
    if label_col is None:
        raise ValueError("label_levels require label_col")
    for value in levels:
        if (not isinstance(value, str) or not value or
                len(value.encode("utf-8", errors="strict")) > 128 or
                not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]*", value) or
                re.match(r"^[A-Za-z][A-Za-z0-9+.-]*:", value)):
            raise ValueError(
                "label_levels must contain only safe public identifiers"
            )
    if len(levels) != len(set(levels)):
        raise ValueError("label_levels must be unique")
    return levels


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
    populated = []
    for candidate, label in (
        ("images", "images"),
        ("source/images", "source/images"),
        (".", "root"),
    ):
        d = os.path.join(source_dir, candidate)
        if os.path.islink(d):
            raise ValueError("symbolic links are not allowed for the image root")
        excluded = (
            {"images", "masks", "labels"}
            if candidate == "." else set()
        )
        if os.path.isdir(d) and _contains_supported_images(
                d, excluded_directories=excluded):
            populated.append((label, d))
    if len(populated) > 1:
        labels = ", ".join(label for label, _ in populated)
        raise ValueError(
            f"multiple populated image roots found: {labels}; keep images under "
            "exactly one of images/, source/images/, or the dataset root"
        )
    return populated[0][1] if populated else None


def _find_masks_dir(source_dir: str) -> str | None:
    """Find the directory containing mask files."""
    populated = []
    for candidate in ["masks", "source/masks", "labels", "source/labels"]:
        d = os.path.join(source_dir, candidate)
        if os.path.islink(d):
            raise ValueError("symbolic links are not allowed for the mask root")
        if os.path.isdir(d) and _contains_supported_masks(d):
            populated.append((candidate, d))
    if len(populated) > 1:
        labels = ", ".join(label for label, _ in populated)
        raise ValueError(
            f"multiple populated mask roots found: {labels}; keep masks under "
            "exactly one of masks/, source/masks/, labels/, or source/labels/"
        )
    return populated[0][1] if populated else None


def _contains_supported_images(
        directory: str, *, excluded_directories: set[str] | None = None) -> bool:
    excluded_directories = excluded_directories or set()
    for entry in os.listdir(directory):
        path = os.path.join(directory, entry)
        if os.path.islink(path):
            raise ValueError("symbolic links are not allowed in image collections")
        if os.path.isfile(path) and is_image_file(entry):
            return True
        if os.path.isdir(path) and entry not in excluded_directories:
            if any(f.lower().endswith(".dcm") for f in os.listdir(path)):
                return True
    return False


def _contains_supported_masks(directory: str) -> bool:
    for root, directories, files in os.walk(directory):
        if any(os.path.islink(os.path.join(root, name)) for name in directories):
            raise ValueError("symbolic links are not allowed in mask collections")
        if any(os.path.islink(os.path.join(root, name)) for name in files):
            raise ValueError("symbolic links are not allowed in mask collections")
        if any(is_image_file(f) for f in files):
            return True
    return False


def _validate_relative_asset_path(path: str) -> None:
    if (not isinstance(path, str) or not path or path.startswith("/") or
            any(char in path for char in ("\\", "\r", "\n"))):
        raise ValueError("collection asset path is invalid")
    if (len(path.encode("utf-8", errors="strict")) > 4096 or
            any(part in {"", ".", ".."} for part in path.split("/"))):
        raise ValueError("collection asset path is invalid")


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


def _validate_image_container_header(filename: str, prefix: bytes, *,
                                     complete: bool) -> bool:
    """Validate a bounded header, returning whether it is complete and inline."""
    lower = filename.lower()
    if lower.endswith(".mhd"):
        raise ValueError(
            "MHD files are not supported; convert to self-contained .mha or NIfTI"
        )
    if lower.endswith(".nrrd"):
        separators = [position for position in (
            prefix.find(b"\n\n"), prefix.find(b"\r\n\r\n")
        ) if position >= 0]
        if not separators:
            if not complete and len(prefix) <= _MAX_MEDICAL_HEADER_BYTES:
                return False
            raise ValueError("NRRD header is invalid or exceeds the safety limit")
        header_end = min(separators)
        if header_end > _MAX_MEDICAL_HEADER_BYTES:
            raise ValueError("NRRD header is invalid or exceeds the safety limit")
        header = prefix[:header_end]
        for line in header.splitlines():
            line = line.strip()
            if not line or line.startswith(b"#") or b":" not in line:
                continue
            key_name = re.sub(
                br"\s+", b"", line.split(b":", 1)[0].strip().lower())
            if key_name == b"datafile":
                raise ValueError(
                    "Detached NRRD files are not supported; "
                    "use a self-contained NRRD or NIfTI"
                )
        return True
    if lower.endswith(".mha"):
        lines = prefix.splitlines()
        if not complete and not prefix.endswith((b"\n", b"\r")):
            lines = lines[:-1]
        for line in lines:
            key_name, separator, value = line.partition(b"=")
            if separator and key_name.strip().lower() == b"elementdatafile":
                if value.strip().upper() != b"LOCAL":
                    raise ValueError(
                        "Detached MetaImage files are not supported; "
                        "use self-contained .mha or NIfTI"
                    )
                return True
        if not complete and len(prefix) <= _MAX_MEDICAL_HEADER_BYTES:
            return False
        raise ValueError("MetaImage header is invalid or exceeds the safety limit")
    return True


def _validate_local_image_container(path: str, filename: str) -> None:
    lower = filename.lower()
    if lower.endswith(".mhd"):
        _validate_image_container_header(filename, b"", complete=True)
    if not lower.endswith((".nrrd", ".mha")):
        return
    with open(path, "rb") as stream:
        prefix = stream.read(_MAX_MEDICAL_HEADER_BYTES + 1)
    _validate_image_container_header(filename, prefix, complete=True)


def _sha256_s3_object(s3, bucket: str, key: str,
                      version_id: str | None = None, *,
                      container_name: str | None = None) -> str:
    if container_name and container_name.lower().endswith(".mhd"):
        _validate_image_container_header(
            container_name, b"", complete=True)
    inspect_header = bool(
        container_name and
        container_name.lower().endswith((".nrrd", ".mha"))
    )
    prefix = bytearray()
    h = hashlib.sha256()
    request = {"Bucket": bucket, "Key": key}
    if version_id not in (None, "", "null"):
        request["VersionId"] = version_id
    response = s3.get_object(**request)
    body = response["Body"]
    try:
        header_complete = not inspect_header
        while not header_complete:
            remaining = _MAX_MEDICAL_HEADER_BYTES + 1 - len(prefix)
            chunk = body.read(min(65536, remaining)) if remaining > 0 else b""
            if chunk:
                h.update(chunk)
                prefix.extend(chunk)
            header_complete = _validate_image_container_header(
                container_name, bytes(prefix), complete=not chunk)
        for chunk in iter(lambda: body.read(65536), b""):
            if chunk:
                h.update(chunk)
    finally:
        body.close()
    return h.hexdigest()
