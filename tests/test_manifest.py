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
    def __init__(self, payload):
        super().__init__(payload)
        self.bytes_read = 0

    def read(self, size=-1):
        chunk = super().read(size)
        self.bytes_read += len(chunk)
        return chunk


class FakeS3:
    def __init__(self, objects):
        self.objects = objects
        self.bodies = []

    def get_object(self, Bucket, Key):
        body = FakeBody(self.objects[Key])
        self.bodies.append(body)
        return {"Body": body}


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
        validate_dataset_id("a" * 128)
        with self.assertRaises(ValueError):
            validate_dataset_id("../bad")
        with self.assertRaises(ValueError):
            validate_dataset_id("study..v1")
        with self.assertRaises(ValueError):
            validate_dataset_id("a" * 129)
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

    def test_scan_images_rejects_multiple_populated_roots(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            roots = (
                os.path.join(tmpdir, "images"),
                os.path.join(tmpdir, "source", "images"),
            )
            for index, root in enumerate(roots, start=1):
                os.makedirs(root)
                with open(os.path.join(root, f"case{index}.nii.gz"), "wb") as f:
                    f.write(f"image-{index}".encode())
            with open(os.path.join(tmpdir, "case3.nii.gz"), "wb") as f:
                f.write(b"image-3")

            with self.assertRaisesRegex(
                    ValueError, "multiple populated image roots.*images.*source/images.*root"):
                scan_images(tmpdir)

    def test_scan_images_does_not_double_count_reserved_dicom_root(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            images = os.path.join(tmpdir, "images")
            os.makedirs(images)
            with open(os.path.join(images, "case001.dcm"), "wb") as f:
                f.write(b"dicom")

            samples = scan_images(tmpdir)

        self.assertEqual([sample["sample_id"] for sample in samples], ["case001"])

    def test_scan_images_keeps_root_dicom_series_named_source(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            series = os.path.join(tmpdir, "source")
            os.makedirs(series)
            with open(os.path.join(series, "001.dcm"), "wb") as f:
                f.write(b"dicom")

            samples = scan_images(tmpdir)

        self.assertEqual([sample["sample_id"] for sample in samples], ["source"])

    def test_scan_masks_rejects_multiple_populated_roots(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            roots = (
                os.path.join(tmpdir, "masks"),
                os.path.join(tmpdir, "source", "masks"),
                os.path.join(tmpdir, "labels"),
                os.path.join(tmpdir, "source", "labels"),
            )
            for index, root in enumerate(roots, start=1):
                os.makedirs(root)
                with open(os.path.join(root, f"case{index}_mask.nii.gz"), "wb") as f:
                    f.write(f"mask-{index}".encode())

            with self.assertRaisesRegex(
                    ValueError,
                    "multiple populated mask roots.*masks.*source/masks.*labels.*source/labels"):
                scan_masks(
                    tmpdir,
                    sample_ids=["case1", "case2", "case3", "case4"],
                )

    def test_local_scans_reject_detached_image_and_mask_containers(self):
        cases = (
            ("mhd", b"ObjectType = Image\nElementDataFile = payload.raw\n",
             "MHD files are not supported"),
            ("nrrd", b"NRRD0005\r\ntype: uchar\r\nDaTa FiLe : LIST\r\n\r\n",
             "Detached NRRD"),
            ("mha", b"ObjectType = Image\nElementDataFile = payload.raw\n",
             "Detached MetaImage"),
        )
        for extension, payload, message in cases:
            with self.subTest(asset="image", extension=extension), \
                    tempfile.TemporaryDirectory() as tmpdir:
                images = os.path.join(tmpdir, "images")
                os.makedirs(images)
                with open(os.path.join(images, f"case.{extension}"), "wb") as f:
                    f.write(payload)
                with open(os.path.join(images, "payload.raw"), "wb") as f:
                    f.write(b"pixels")
                with self.assertRaisesRegex(ValueError, message):
                    scan_images(tmpdir)

            with self.subTest(asset="mask", extension=extension), \
                    tempfile.TemporaryDirectory() as tmpdir:
                masks = os.path.join(tmpdir, "masks")
                os.makedirs(masks)
                with open(os.path.join(masks, f"case_mask.{extension}"), "wb") as f:
                    f.write(payload)
                with open(os.path.join(masks, "payload.raw"), "wb") as f:
                    f.write(b"pixels")
                with self.assertRaisesRegex(ValueError, message):
                    scan_masks(tmpdir, sample_ids=["case"])

    def test_local_scans_accept_inline_nrrd_mha_images_and_masks(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            images = os.path.join(tmpdir, "images")
            masks = os.path.join(tmpdir, "masks")
            os.makedirs(images)
            os.makedirs(masks)
            payloads = {
                "inline_nrrd.nrrd": (
                    b"NRRD0005\ntype: uchar\ndimension: 2\nsizes: 1 1\n"
                    b"encoding: raw\n\n\x01"
                ),
                "inline_mha.mha": (
                    b"ObjectType = Image\r\nNDims = 2\r\nDimSize = 1 1\r\n"
                    b"ElementType = MET_UCHAR\r\neLeMeNtDaTaFiLe   =   local\r\n\x01"
                ),
            }
            for filename, payload in payloads.items():
                with open(os.path.join(images, filename), "wb") as f:
                    f.write(payload)
                stem, extension = filename.split(".", 1)
                with open(os.path.join(masks, f"{stem}_mask.{extension}"), "wb") as f:
                    f.write(payload)

            samples = scan_images(tmpdir)
            scanned_masks = scan_masks(
                tmpdir, sample_ids=[sample["sample_id"] for sample in samples])

        self.assertEqual(
            [sample["sample_id"] for sample in samples],
            ["inline_mha", "inline_nrrd"],
        )
        self.assertEqual(
            [mask["sample_id"] for mask in scanned_masks],
            ["inline_mha", "inline_nrrd"],
        )

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

    def test_s3_scans_used_by_controller_reject_detached_containers(self):
        bucket = "imaging-data"
        prefix = "datasets/study_ct_v1"
        cases = (
            ("mhd", b"ObjectType = Image\nElementDataFile = payload.raw\n",
             "MHD files are not supported"),
            ("nrrd", b"NRRD0005\r\ntype: uchar\r\nDaTa FiLe : LIST\r\n\r\n",
             "Detached NRRD"),
            ("mha", b"ObjectType = Image\nElementDataFile = payload.raw\n",
             "Detached MetaImage"),
        )
        for extension, payload, message in cases:
            image_key = f"{prefix}/source/images/case.{extension}"
            with self.subTest(asset="image", extension=extension):
                with self.assertRaisesRegex(ValueError, message):
                    scan_s3_images(
                        FakeS3({image_key: payload}), bucket, prefix,
                        [{"key": image_key, "size": len(payload)}],
                    )

            mask_key = f"{prefix}/source/masks/case_mask.{extension}"
            with self.subTest(asset="mask", extension=extension):
                with self.assertRaisesRegex(ValueError, message):
                    scan_s3_masks(
                        FakeS3({mask_key: payload}), bucket, prefix,
                        [{"key": mask_key, "size": len(payload)}],
                        sample_ids=["case"],
                    )

    def test_s3_detached_headers_stop_reading_before_large_payloads(self):
        bucket = "imaging-data"
        prefix = "datasets/study_ct_v1"
        cases = (
            ("nrrd", b"NRRD0005\ntype: uchar\ndatafile: LIST\n\n"),
            ("mha", b"ObjectType = Image\nElementDataFile = payload.raw\n"),
        )
        for extension, header in cases:
            payload = header + b"x" * (2 * 1024 * 1024)
            key = f"{prefix}/source/images/case.{extension}"
            s3 = FakeS3({key: payload})

            with self.subTest(extension=extension), \
                    self.assertRaisesRegex(ValueError, "Detached"):
                scan_s3_images(
                    s3, bucket, prefix,
                    [{"key": key, "size": len(payload)}],
                )

            self.assertEqual(len(s3.bodies), 1)
            self.assertLessEqual(s3.bodies[0].bytes_read, 65536)
            self.assertLess(s3.bodies[0].bytes_read, len(payload))
            self.assertTrue(s3.bodies[0].closed)

    def test_s3_malformed_headers_are_bounded_by_the_header_limit(self):
        bucket = "imaging-data"
        prefix = "datasets/study_ct_v1"
        cases = (
            ("nrrd", b"NRRD0005\n" + b"# comment\n" * 200000),
            ("mha", b"ObjectType = Image\n" + b"Comment = value\n" * 150000),
        )
        for extension, payload in cases:
            key = f"{prefix}/source/images/case.{extension}"
            s3 = FakeS3({key: payload})

            with self.subTest(extension=extension), \
                    self.assertRaisesRegex(ValueError, "safety limit"):
                scan_s3_images(
                    s3, bucket, prefix,
                    [{"key": key, "size": len(payload)}],
                )

            self.assertLessEqual(s3.bodies[0].bytes_read, 1024 * 1024 + 1)
            self.assertLess(s3.bodies[0].bytes_read, len(payload))
            self.assertTrue(s3.bodies[0].closed)

    def test_s3_mhd_is_rejected_without_opening_the_object(self):
        bucket = "imaging-data"
        prefix = "datasets/study_ct_v1"
        key = f"{prefix}/source/images/case.mhd"
        s3 = FakeS3({
            key: b"ObjectType = Image\nElementDataFile = LOCAL\n\x01",
        })

        with self.assertRaisesRegex(ValueError, "MHD files are not supported"):
            scan_s3_images(
                s3, bucket, prefix,
                [{"key": key, "size": len(s3.objects[key])}],
            )

        self.assertEqual(s3.bodies, [])

    def test_s3_scans_accept_and_hash_versioned_inline_images_and_masks(self):
        bucket = "imaging-data"
        prefix = "datasets/study_ct_v1"
        inline = {
            "inline_nrrd.nrrd": (
                b"NRRD0005\ntype: uchar\ndimension: 2\nsizes: 1 1\n"
                b"encoding: raw\n\n\x01"
            ),
            "inline_mha.mha": (
                b"ObjectType = Image\r\nNDims = 2\r\nDimSize = 1 1\r\n"
                b"ElementType = MET_UCHAR\r\neLeMeNtDaTaFiLe   =   local\r\n\x01"
            ),
        }
        image_payloads = {
            f"{prefix}/source/images/{name}": payload
            for name, payload in inline.items()
        }
        mask_payloads = {
            f"{prefix}/source/masks/{name.replace('.', '_mask.', 1)}": payload
            for name, payload in inline.items()
        }
        payloads = image_payloads | mask_payloads
        versions = {(key, "version-1"): payload
                    for key, payload in payloads.items()}
        s3 = VersionedFakeS3(
            {key: b"different-current-value" for key in payloads}, versions)
        image_objects = [
            {"key": key, "size": len(payload), "version_id": "version-1"}
            for key, payload in image_payloads.items()
        ]
        mask_objects = [
            {"key": key, "size": len(payload), "version_id": "version-1"}
            for key, payload in mask_payloads.items()
        ]

        samples = scan_s3_images(s3, bucket, prefix, image_objects)
        masks = scan_s3_masks(
            s3, bucket, prefix, mask_objects,
            sample_ids=[sample["sample_id"] for sample in samples],
        )

        self.assertEqual(
            [sample["sample_id"] for sample in samples],
            ["inline_mha", "inline_nrrd"],
        )
        self.assertEqual(
            [mask["sample_id"] for mask in masks],
            ["inline_mha", "inline_nrrd"],
        )
        expected_hashes = {
            hashlib.sha256(payload).hexdigest() for payload in inline.values()
        }
        self.assertEqual(
            {item["content_hash"] for item in samples + masks}, expected_hashes)
        self.assertTrue(all(
            item["content_hash_version_id"] == "version-1"
            for item in samples + masks
        ))
        self.assertCountEqual(
            s3.requests,
            [(key, "version-1") for key in payloads],
        )

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
