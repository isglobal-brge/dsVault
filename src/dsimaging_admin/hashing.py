"""Content hashing utilities."""

import hashlib
import os

HASH_CHUNK = 65536
IMAGE_EXTENSIONS = frozenset({
    ".nii.gz", ".nii", ".nrrd", ".mha", ".mhd", ".dcm",
    ".svs", ".tif", ".tiff", ".png", ".jpg",
})


def sha256_file(path: str) -> str:
    """Streaming SHA-256 of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(HASH_CHUNK)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def is_image_file(filename: str) -> bool:
    lower = filename.lower()
    return any(lower.endswith(ext) for ext in IMAGE_EXTENSIONS)


def sample_id_from_filename(filename: str) -> str:
    """Strip known extensions to get sample_id."""
    for ext in sorted(IMAGE_EXTENSIONS, key=len, reverse=True):
        if filename.lower().endswith(ext):
            return filename[: -len(ext)]
    return os.path.splitext(filename)[0]
