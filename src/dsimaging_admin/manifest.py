"""Manifest and index generation."""

import json
import os
import tempfile
import time

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from .hashing import sha256_file, is_image_file, sample_id_from_filename


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
                import hashlib
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
                    "files": [{"path": f"{entry}/{f}", "role": "slice"} for f in dcm_files],
                    "content_hash": h.hexdigest(),
                    "size": total_size,
                    "local_path": filepath,
                })

    return samples


def generate_manifest(dataset_id: str, bucket: str, prefix: str,
                      modality: str = "unknown") -> dict:
    """Generate a manifest dict for a dataset."""
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
            f"s3://{bucket}/{prefix}/source/images/{s['primary_filename']}"
            if s["primary_filename"]
            else f"s3://{bucket}/{prefix}/source/images/{s['sample_id']}/"
            for s in samples
        ],
        "content_hash": [s["content_hash"] for s in samples],
        "size": pa.array([s["size"] for s in samples], type=pa.int64()),
        "last_modified": [now for _ in samples],
        "version_id": pa.array([None for _ in samples], type=pa.string()),
        "etag": pa.array([None for _ in samples], type=pa.string()),
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
        if os.path.isdir(d):
            files = [f for f in os.listdir(d) if is_image_file(f)]
            if files:
                return d
    # Check for DICOM subdirectories
    for sub in os.listdir(source_dir):
        subdir = os.path.join(source_dir, sub)
        if os.path.isdir(subdir):
            if any(f.lower().endswith(".dcm") for f in os.listdir(subdir)):
                return source_dir
    return None
