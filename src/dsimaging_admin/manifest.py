"""Manifest and index generation."""

import hashlib
import json
import os
import re
import time

import pyarrow as pa
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


def generate_manifest(dataset_id: str, bucket: str, prefix: str,
                      modality: str = "unknown") -> dict:
    """Generate a manifest dict for a dataset."""
    validate_dataset_id(dataset_id)
    return {
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


def write_manifest_yaml(manifest: dict, path: str):
    with open(path, "w") as f:
        yaml.dump(manifest, f, default_flow_style=False, sort_keys=False)


def build_hash_index(samples: list[dict], bucket: str, prefix: str) -> pa.Table:
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ")
    return pa.table({
        "sample_id": [s["sample_id"] for s in samples],
        "uri": [
            f"s3://{bucket}/{prefix}/source/images/{s.get('uri_path') or s['primary_filename']}"
            if s.get("uri_path") or s["primary_filename"]
            else f"s3://{bucket}/{prefix}/source/images/{s['sample_id']}/"
            for s in samples
        ],
        "content_hash": [s["content_hash"] for s in samples],
        "size": pa.array([s["size"] for s in samples], type=pa.int64()),
        "last_modified": [s.get("last_modified") or now for s in samples],
        "version_id": pa.array([s.get("version_id") for s in samples], type=pa.string()),
        "etag": pa.array([s.get("etag") for s in samples], type=pa.string()),
        "source_kind": [s["source_kind"] for s in samples],
    })


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


def build_samples_metadata(samples: list[dict]) -> pa.Table:
    return pa.table({
        "sample_id": [s["sample_id"] for s in samples],
        "source_kind": [s["source_kind"] for s in samples],
        "n_files": pa.array([len(s["files"]) for s in samples], type=pa.int32()),
    })


def _find_images_dir(source_dir: str) -> str | None:
    """Find the directory containing image files."""
    for candidate in ["images", "source/images", "."]:
        d = os.path.join(source_dir, candidate)
        if os.path.isdir(d) and _contains_supported_images(d):
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
