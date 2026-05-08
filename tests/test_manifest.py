import io
import os
import tempfile
import unittest

from dsimaging_admin.manifest import (
    build_hash_index,
    build_sample_manifests,
    scan_images,
    scan_s3_images,
    validate_dataset_id,
)


class FakeBody(io.BytesIO):
    pass


class FakeS3:
    def __init__(self, objects):
        self.objects = objects

    def get_object(self, Bucket, Key):
        return {"Body": FakeBody(self.objects[Key])}


class ManifestTests(unittest.TestCase):
    def test_validate_dataset_id_rejects_unsafe_values(self):
        validate_dataset_id("lung1_site-a.v1")
        with self.assertRaises(ValueError):
            validate_dataset_id("../bad")
        with self.assertRaises(ValueError):
            validate_dataset_id("Bad ID")

    def test_scan_images_finds_source_images_dicom_series(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            series_dir = os.path.join(tmpdir, "source", "images", "case001")
            os.makedirs(series_dir)
            with open(os.path.join(series_dir, "001.dcm"), "wb") as f:
                f.write(b"slice-1")
            with open(os.path.join(series_dir, "002.dcm"), "wb") as f:
                f.write(b"slice-2")

            samples = scan_images(tmpdir)

        self.assertEqual(len(samples), 1)
        self.assertEqual(samples[0]["sample_id"], "case001")
        self.assertEqual(samples[0]["source_kind"], "dicom_series")
        self.assertEqual(len(samples[0]["files"]), 2)

    def test_scan_s3_images_groups_single_files_and_dicom(self):
        bucket = "imaging-data"
        prefix = "datasets/lung"
        payloads = {
            f"{prefix}/source/images/a.nii.gz": b"nifti",
            f"{prefix}/source/images/series1/001.dcm": b"one",
            f"{prefix}/source/images/series1/002.dcm": b"two",
        }
        objects = [
            {"key": key, "size": len(value), "last_modified": "2026-05-08T00:00:00Z"}
            for key, value in payloads.items()
        ]

        samples = scan_s3_images(FakeS3(payloads), bucket, prefix, objects)
        sample_ids = [sample["sample_id"] for sample in samples]

        self.assertEqual(sample_ids, ["a", "series1"])
        self.assertEqual(samples[0]["source_kind"], "single_file")
        self.assertEqual(samples[0]["uri_path"], "a.nii.gz")
        self.assertEqual(samples[1]["source_kind"], "dicom_series")
        self.assertEqual(len(samples[1]["files"]), 2)

    def test_build_tables_use_canonical_uris(self):
        samples = [{
            "sample_id": "a",
            "source_kind": "single_file",
            "primary_filename": "a.nii.gz",
            "uri_path": "a.nii.gz",
            "files": [{"path": "a.nii.gz", "role": "primary"}],
            "content_hash": "abc",
            "size": 5,
        }]

        hash_index = build_hash_index(samples, "imaging-data", "datasets/lung")
        manifests = build_sample_manifests(samples)

        self.assertEqual(
            hash_index.to_pydict()["uri"][0],
            "s3://imaging-data/datasets/lung/source/images/a.nii.gz",
        )
        self.assertEqual(manifests.to_pydict()["n_files"][0], 1)


if __name__ == "__main__":
    unittest.main()
