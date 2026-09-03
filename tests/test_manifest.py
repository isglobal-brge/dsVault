import io
import hashlib
import os
import tempfile
import unittest

from dsimaging_admin.manifest import (
    build_hash_index,
    build_mask_hash_index,
    build_sample_manifests,
    build_samples_metadata,
    generate_manifest,
    metadata_contract,
    metadata_contract_from_manifest,
    read_metadata_table,
    scan_images,
    scan_masks,
    scan_s3_images,
    scan_s3_masks,
    validate_dataset_id,
)


class FakeBody(io.BytesIO):
    pass


class FakeS3:
    def __init__(self, objects):
        self.objects = objects

    def get_object(self, Bucket, Key):
        return {"Body": FakeBody(self.objects[Key])}


class VersionedFakeS3:
    def __init__(self, current, versions):
        self.current = current
        self.versions = versions
        self.requests = []

    def get_object(self, Bucket, Key, VersionId=None):
        self.requests.append((Key, VersionId))
        payload = (self.current[Key] if VersionId is None else
                   self.versions[(Key, VersionId)])
        return {"Body": FakeBody(payload)}


class ManifestTests(unittest.TestCase):
    def test_validate_dataset_id_rejects_unsafe_values(self):
        validate_dataset_id("study_ct_v1_site-a.v1")
        with self.assertRaises(ValueError):
            validate_dataset_id("../bad")
        with self.assertRaises(ValueError):
            validate_dataset_id("Bad ID")
        with self.assertRaises(ValueError):
            validate_dataset_id("study\n")

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
        prefix = "datasets/study_ct_v1"
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

    def test_scan_s3_assets_hash_the_recorded_object_versions(self):
        bucket = "imaging-data"
        prefix = "datasets/study_ct_v1"
        image_key = f"{prefix}/source/images/a.nii.gz"
        slice_keys = [
            f"{prefix}/source/images/series1/001.dcm",
            f"{prefix}/source/images/series1/002.dcm",
        ]
        mask_key = f"{prefix}/source/masks/a_mask.nii.gz"
        keys = [image_key, *slice_keys, mask_key]
        versions = {(key, "version-1"): f"old-{key}".encode()
                    for key in keys}
        current = {key: f"new-{key}".encode() for key in keys}
        s3 = VersionedFakeS3(current, versions)
        image_objects = [
            {"key": key, "size": len(versions[(key, "version-1")]),
             "last_modified": "2026-05-08T00:00:00Z",
             "version_id": "version-1"}
            for key in [image_key, *slice_keys]
        ]
        mask_objects = [{
            "key": mask_key,
            "size": len(versions[(mask_key, "version-1")]),
            "last_modified": "2026-05-08T00:00:00Z",
            "version_id": "version-1",
        }]

        samples = scan_s3_images(s3, bucket, prefix, image_objects)
        masks = scan_s3_masks(
            s3, bucket, prefix, mask_objects, sample_ids=["a", "series1"])

        expected_image = hashlib.sha256(
            versions[(image_key, "version-1")]).hexdigest()
        expected_series = hashlib.sha256()
        for key in slice_keys:
            expected_series.update(hashlib.sha256(
                versions[(key, "version-1")]).hexdigest().encode())
        self.assertEqual(samples[0]["content_hash"], expected_image)
        self.assertEqual(samples[1]["content_hash"], expected_series.hexdigest())
        self.assertEqual(
            masks[0]["content_hash"],
            hashlib.sha256(versions[(mask_key, "version-1")]).hexdigest(),
        )
        self.assertEqual(s3.requests, [(key, "version-1") for key in keys])

    def test_scan_masks_maps_common_suffixes_to_image_samples(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            masks_dir = os.path.join(tmpdir, "source", "masks")
            os.makedirs(masks_dir)
            with open(os.path.join(masks_dir, "case001_GTV-1.nii.gz"), "wb") as f:
                f.write(b"mask-1")
            with open(os.path.join(masks_dir, "case002_mask.nii.gz"), "wb") as f:
                f.write(b"mask-2")

            masks = scan_masks(tmpdir, sample_ids=["case001", "case002"])

        self.assertEqual([mask["sample_id"] for mask in masks], ["case001", "case002"])
        self.assertEqual(masks[0]["source_kind"], "mask_file")
        self.assertEqual(masks[0]["uri_path"], "case001_GTV-1.nii.gz")

    def test_scan_masks_rejects_orphans_and_duplicate_sample_mappings(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            masks_dir = os.path.join(tmpdir, "source", "masks")
            os.makedirs(masks_dir)
            with open(os.path.join(masks_dir, "unknown_mask.nii.gz"), "wb") as f:
                f.write(b"mask")
            with self.assertRaisesRegex(ValueError, "no matching image"):
                scan_masks(tmpdir, sample_ids=["case001"])

        with tempfile.TemporaryDirectory() as tmpdir:
            masks_dir = os.path.join(tmpdir, "source", "masks")
            os.makedirs(masks_dir)
            for name in ("case001_mask.nii.gz", "case001_seg.nii.gz"):
                with open(os.path.join(masks_dir, name), "wb") as f:
                    f.write(name.encode())
            with self.assertRaisesRegex(ValueError, "duplicate sample_id"):
                scan_masks(tmpdir, sample_ids=["case001"])

    def test_scan_s3_masks_and_hash_index_use_mask_uris(self):
        bucket = "imaging-data"
        prefix = "datasets/study_ct_v1"
        payloads = {
            f"{prefix}/source/masks/case001_GTV-1.nii.gz": b"mask",
        }
        objects = [
            {"key": key, "size": len(value), "last_modified": "2026-05-08T00:00:00Z"}
            for key, value in payloads.items()
        ]

        masks = scan_s3_masks(
            FakeS3(payloads), bucket, prefix, objects,
            sample_ids=["case001"],
        )
        mask_index = build_mask_hash_index(masks, bucket, prefix)

        self.assertEqual(masks[0]["sample_id"], "case001")
        self.assertEqual(
            mask_index.to_pydict()["uri"][0],
            "s3://imaging-data/datasets/study_ct_v1/source/masks/case001_GTV-1.nii.gz",
        )

    def test_generate_manifest_can_declare_mask_asset(self):
        manifest = generate_manifest(
            "study_ct_v1",
            "imaging-data",
            "datasets/study_ct_v1",
            modality="ct",
            has_masks=True,
            privacy_unit_col="patient_id",
            label_col="diagnosis",
            label_levels=["case", "control"],
        )

        self.assertEqual(manifest["assets"]["masks"]["kind"], "mask_root")
        self.assertEqual(
            manifest["assets"]["masks"]["content_hash_index"],
            "s3://imaging-data/datasets/study_ct_v1/indexes/masks_content_hash_index.parquet",
        )
        self.assertEqual(metadata_contract_from_manifest(manifest), {
            "id_col": "sample_id",
            "privacy_unit": "patient",
            "privacy_unit_col": "patient_id",
            "privacy_unit_canonicalization": "trim-utf8-v2",
            "label_col": "diagnosis",
            "label_levels": ["case", "control"],
        })

    def test_generate_manifest_rejects_preserved_cross_collection_asset(self):
        source = generate_manifest(
            "source", "imaging-data", "datasets/source",
            privacy_unit_col="patient_id",
        )
        source["assets"]["extra"] = {
            "uri": "s3://imaging-data/datasets/source/source/extra/",
        }

        with self.assertRaisesRegex(ValueError, "outside its collection"):
            generate_manifest(
                "target", "imaging-data", "datasets/target",
                privacy_unit_col="patient_id", existing_manifest=source,
            )

        source["assets"].pop("extra")
        source["metadata"]["file"] = (
            "s3://imaging-data/datasets/source/metadata/legacy.parquet")
        regenerated = generate_manifest(
            "source", "imaging-data", "datasets/source",
            privacy_unit_col="patient_id", existing_manifest=source,
        )
        self.assertNotIn("file", regenerated["metadata"])

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

        hash_index = build_hash_index(samples, "imaging-data", "datasets/study_ct_v1")
        manifests = build_sample_manifests(samples)

        self.assertEqual(
            hash_index.to_pydict()["uri"][0],
            "s3://imaging-data/datasets/study_ct_v1/source/images/a.nii.gz",
        )
        self.assertEqual(manifests.to_pydict()["primary_uri"][0], "a.nii.gz")
        self.assertEqual(manifests.to_pydict()["n_files"][0], 1)

        nested = [{
            **samples[0],
            "uri_path": "site-a/a.nii.gz",
            "files": [{"path": "site-a/a.nii.gz", "role": "primary"}],
        }]
        nested_manifests = build_sample_manifests(nested)
        self.assertEqual(
            nested_manifests.to_pydict()["primary_uri"][0],
            "site-a/a.nii.gz",
        )

    def test_samples_metadata_can_include_clinical_columns(self):
        samples = [{
            "sample_id": "case001",
            "source_kind": "single_file",
            "primary_filename": "case001.nii.gz",
            "uri_path": "case001.nii.gz",
            "files": [{"path": "case001.nii.gz", "role": "primary"}],
            "content_hash": "abc",
            "size": 5,
        }]

        with tempfile.TemporaryDirectory() as tmpdir:
            metadata_path = os.path.join(tmpdir, "clinical.csv")
            with open(metadata_path, "w", encoding="utf-8") as f:
                f.write("sample_id,age,deadstatus_event\n")
                f.write("case001,68,1\n")

            metadata = read_metadata_table(metadata_path)
            table = build_samples_metadata(samples, extra_metadata=metadata)

        values = table.to_pydict()
        self.assertEqual(values["sample_id"], ["case001"])
        self.assertEqual(values["age"], [68])
        self.assertEqual(values["deadstatus_event"], [1])

    def test_samples_metadata_rejects_rows_outside_image_roster(self):
        samples = [{
            "sample_id": "case001",
            "source_kind": "single_file",
            "primary_filename": "case001.nii.gz",
            "uri_path": "case001.nii.gz",
            "files": [{"path": "case001.nii.gz", "role": "primary"}],
            "content_hash": "abc",
            "size": 5,
        }]
        metadata = read_metadata_table(self._metadata_csv(
            "sample_id,patient_id\n"
            "case001,patient-a\n"
            "case999,patient-b\n"
        ))

        with self.assertRaisesRegex(ValueError, "roster must exactly match"):
            build_samples_metadata(
                samples, extra_metadata=metadata,
                privacy_unit_col="patient_id",
            )

    def test_samples_metadata_rejects_duplicate_clinical_rows(self):
        samples = [
            {
                "sample_id": "case001",
                "source_kind": "single_file",
                "primary_filename": "case001.nii.gz",
                "uri_path": "case001.nii.gz",
                "files": [{"path": "case001.nii.gz", "role": "primary"}],
                "content_hash": "abc",
                "size": 5,
            },
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            metadata_path = os.path.join(tmpdir, "clinical.csv")
            with open(metadata_path, "w", encoding="utf-8") as f:
                f.write("sample_id,age\ncase001,68\ncase001,70\n")

            metadata = read_metadata_table(metadata_path)
            with self.assertRaises(ValueError):
                build_samples_metadata(samples, extra_metadata=metadata)

    def test_samples_metadata_requires_complete_patient_and_declared_label(self):
        samples = [
            {
                "sample_id": sample_id,
                "source_kind": "single_file",
                "primary_filename": f"{sample_id}.nii.gz",
                "uri_path": f"{sample_id}.nii.gz",
                "files": [{"path": f"{sample_id}.nii.gz", "role": "primary"}],
                "content_hash": sample_id,
                "size": 1,
            }
            for sample_id in ("case001", "case002")
        ]
        metadata = read_metadata_table(self._metadata_csv(
            "sample_id,patient_id,label\ncase001,patient-a,A\ncase002,patient-a,B\n"
        ))
        table = build_samples_metadata(
            samples, extra_metadata=metadata,
            privacy_unit_col="patient_id", label_col="label",
        )
        self.assertEqual(table["patient_id"].to_pylist(), ["patient-a", "patient-a"])

        incomplete = read_metadata_table(self._metadata_csv(
            "sample_id,patient_id,label\ncase001,patient-a,A\ncase002,,B\n"
        ))
        with self.assertRaisesRegex(ValueError, "patient_id is empty"):
            build_samples_metadata(
                samples, extra_metadata=incomplete,
                privacy_unit_col="patient_id", label_col="label",
            )

    def test_patient_ids_are_written_in_canonical_form(self):
        samples = [
            {
                "sample_id": sample_id,
                "source_kind": "single_file",
                "primary_filename": f"{sample_id}.nii.gz",
                "uri_path": f"{sample_id}.nii.gz",
                "files": [{"path": f"{sample_id}.nii.gz", "role": "primary"}],
                "content_hash": sample_id,
                "size": 1,
            }
            for sample_id in ("case001", "case002")
        ]
        metadata = read_metadata_table(self._metadata_csv(
            "sample_id,patient_id\ncase001, patient-a \ncase002,patient-b\t\n"
        ))

        table = build_samples_metadata(
            samples, extra_metadata=metadata,
            privacy_unit_col=" patient_id ",
        )

        self.assertEqual(
            table["patient_id"].to_pylist(), ["patient-a", "patient-b"])

    def test_privacy_and_label_columns_must_be_dedicated(self):
        with self.assertRaisesRegex(ValueError, "dedicated patient column"):
            metadata_contract("sample_id")
        with self.assertRaisesRegex(ValueError, "dedicated patient column"):
            metadata_contract("source_kind")
        with self.assertRaisesRegex(ValueError, "dedicated patient column"):
            metadata_contract("Source_Kind")
        with self.assertRaisesRegex(ValueError, "distinct"):
            metadata_contract("patient_id", "patient_id")
        with self.assertRaisesRegex(ValueError, "distinct"):
            metadata_contract("patient_id", "n_files")

    def test_public_label_vocabulary_is_explicit_and_safe(self):
        contract = metadata_contract(
            "patient_id", "label", ["case", "control"])
        self.assertEqual(contract["label_levels"], ["case", "control"])
        with self.assertRaisesRegex(ValueError, "require label_col"):
            metadata_contract("patient_id", label_levels=["case"])
        with self.assertRaisesRegex(ValueError, "safe public"):
            metadata_contract("patient_id", "label", ["s3://secret"])
        with self.assertRaisesRegex(ValueError, "unique"):
            metadata_contract("patient_id", "label", ["case", "case"])

    def test_samples_metadata_enforces_public_label_vocabulary(self):
        samples = [
            {
                "sample_id": sample_id,
                "source_kind": "single_file",
                "primary_filename": f"{sample_id}.nii.gz",
                "uri_path": f"{sample_id}.nii.gz",
                "files": [{"path": f"{sample_id}.nii.gz", "role": "primary"}],
                "content_hash": sample_id,
                "size": 1,
            }
            for sample_id in ("case001", "case002")
        ]
        metadata = read_metadata_table(self._metadata_csv(
            "sample_id,patient_id,label\ncase001,patient-a,case\n"
            "case002,patient-b,control\n"
        ))
        build_samples_metadata(
            samples, extra_metadata=metadata, privacy_unit_col="patient_id",
            label_col="label", label_levels=["case", "control"],
        )
        with self.assertRaisesRegex(ValueError, "public label vocabulary"):
            build_samples_metadata(
                samples, extra_metadata=metadata,
                privacy_unit_col="patient_id", label_col="label",
                label_levels=["case"],
            )
        with self.assertRaisesRegex(ValueError, "sample or patient identifiers"):
            build_samples_metadata(
                samples, extra_metadata=metadata,
                privacy_unit_col="patient_id", label_col="label",
                label_levels=["case", "control", "patient-a"],
            )
        with self.assertRaisesRegex(ValueError, "sample or patient identifiers"):
            build_samples_metadata(
                samples, extra_metadata=metadata,
                privacy_unit_col="patient_id", label_col="label",
                label_levels=["case", "control", "case001"],
            )

    def test_sample_ids_that_change_under_canonicalization_are_rejected(self):
        samples = [{
            "sample_id": " case001 ",
            "source_kind": "single_file",
            "primary_filename": " case001 .nii.gz",
            "uri_path": " case001 .nii.gz",
            "files": [{"path": " case001 .nii.gz", "role": "primary"}],
            "content_hash": "abc",
            "size": 1,
        }]

        with self.assertRaisesRegex(ValueError, "already satisfy trim-utf8-v2"):
            build_samples_metadata(samples)

        clean_samples = [{**samples[0], "sample_id": "case001"}]
        metadata = read_metadata_table(self._metadata_csv(
            "sample_id,patient_id\n case001 ,patient-a\n"
        ))
        with self.assertRaisesRegex(ValueError, "already satisfy trim-utf8-v2"):
            build_samples_metadata(
                clean_samples, extra_metadata=metadata,
                privacy_unit_col="patient_id",
            )

    def test_scan_s3_images_rejects_duplicate_ids_across_collections(self):
        bucket = "imaging-data"
        prefix = "datasets/study_ct_v1"
        payloads = {
            f"{prefix}/source/images/site-a/case001.nii.gz": b"a",
            f"{prefix}/source/images/site-b/case001.nii.gz": b"b",
        }
        objects = [
            {"key": key, "size": len(value)} for key, value in payloads.items()
        ]
        with self.assertRaisesRegex(ValueError, "duplicate sample_id"):
            scan_s3_images(FakeS3(payloads), bucket, prefix, objects)

    def test_scans_reject_traversal_keys_and_symbolic_links(self):
        prefix = "datasets/study_ct_v1"
        key = f"{prefix}/source/images/../other/case001.nii.gz"
        with self.assertRaisesRegex(ValueError, "asset path is invalid"):
            scan_s3_images(
                FakeS3({key: b"image"}), "imaging-data", prefix,
                [{"key": key, "size": 5}],
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            images = os.path.join(tmpdir, "images")
            os.makedirs(images)
            target = os.path.join(tmpdir, "outside.nii.gz")
            with open(target, "wb") as stream:
                stream.write(b"image")
            try:
                os.symlink(target, os.path.join(images, "case001.nii.gz"))
            except (OSError, NotImplementedError):
                return
            with self.assertRaisesRegex(ValueError, "symbolic links"):
                scan_images(tmpdir)

    def _metadata_csv(self, contents: str) -> str:
        handle = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", encoding="utf-8", delete=False
        )
        self.addCleanup(lambda: os.path.exists(handle.name) and os.unlink(handle.name))
        with handle:
            handle.write(contents)
        return handle.name


if __name__ == "__main__":
    unittest.main()
