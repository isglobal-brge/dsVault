"""Manifest and index generation."""

from __future__ import annotations

import hashlib
import json
import os
import re
import time

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import yaml

from .hashing import sha256_file, is_image_file, sample_id_from_filename

DATASET_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]*$")


def validate_dataset_id(dataset_id: str) -> str:
    """Validate and return a dsimaging dataset identifier."""
    if not DATASET_ID_RE.match(dataset_id or ""):
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

    return samples


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

    return sorted(masks, key=lambda sample: sample["sample_id"])


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

    return sorted(samples, key=lambda sample: sample["sample_id"])


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

    return sorted(masks, key=lambda sample: sample["sample_id"])


def generate_manifest(dataset_id: str, bucket: str, prefix: str,
                      modality: str = "unknown", has_masks: bool = False,
                      mask_asset: str = "masks") -> dict:
    """Generate a manifest dict for a dataset."""
    validate_dataset_id(dataset_id)
    manifest = {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "modality": modality,
        "assets": {
            "images": {
                "uri": f"s3://{bucket}/{prefix}/source/images/",
                "kind": "image_root",
            },
        },
        "metadata": {
            "uri": f"s3://{bucket}/{prefix}/metadata/samples.parquet",
            "format": "parquet",
        },
        "content_hash_index": {
            "uri": f"s3://{bucket}/{prefix}/indexes/content_hash_index.parquet",
            "format": "parquet",
        },
        "sample_manifests": {
            "uri": f"s3://{bucket}/{prefix}/metadata/sample_manifests.parquet",
            "format": "parquet",
        },
    }
    if has_masks:
        manifest["assets"][mask_asset] = {
            "uri": f"s3://{bucket}/{prefix}/source/masks/",
            "kind": "mask_root",
            "content_hash_index": (
                f"s3://{bucket}/{prefix}/indexes/"
                f"{mask_asset}_content_hash_index.parquet"
            ),
        }
    return manifest


def write_manifest_yaml(manifest: dict, path: str):
    with open(path, "w") as f:
        yaml.dump(manifest, f, default_flow_style=False, sort_keys=False)


def build_hash_index(samples: list[dict], bucket: str, prefix: str,
                     source_path: str = "images") -> pa.Table:
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ")
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
    return pa.table({
        "sample_id": [s["sample_id"] for s in samples],
        "source_kind": [s["source_kind"] for s in samples],
        "primary_uri": pa.array(
            [s["primary_filename"] for s in samples], type=pa.string()
        ),
        "files_json": [json.dumps(s["files"]) for s in samples],
        "content_hash": [s["content_hash"] for s in samples],
        "n_files": pa.array([len(s["files"]) for s in samples], type=pa.int32()),
    })


def build_samples_metadata(samples: list[dict],
                           extra_metadata: pa.Table | None = None) -> pa.Table:
    """Build sample metadata, optionally left-joining user metadata.

    ``extra_metadata`` must contain a unique ``sample_id`` column. Extra rows
    that do not correspond to discovered images are ignored; missing rows for
    image samples become nulls. This keeps the manifest image-led while allowing
    clinical/outcome variables to travel with the dataset.
    """
    base = pa.table({
        "sample_id": [s["sample_id"] for s in samples],
        "source_kind": [s["source_kind"] for s in samples],
        "n_files": pa.array([len(s["files"]) for s in samples], type=pa.int32()),
    })
    if extra_metadata is None:
        return base
    return _left_join_metadata(base, extra_metadata)


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


def _left_join_metadata(base: pa.Table, extra_metadata: pa.Table) -> pa.Table:
    extra_metadata = _normalise_metadata_table(extra_metadata)
    base_ids = base["sample_id"].to_pylist()
    rows_by_id: dict[str, dict] = {}
    for row in extra_metadata.to_pylist():
        sample_id = row.get("sample_id")
        if sample_id in rows_by_id:
            raise ValueError(f"metadata has duplicate sample_id: {sample_id}")
        rows_by_id[sample_id] = row

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
