import json
import os
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from click.testing import CliRunner

try:
    import boto3
    from moto import mock_aws
    HAS_MOTO = True
except Exception:
    boto3 = None
    mock_aws = None
    HAS_MOTO = False

from dsimaging_admin.cli import (
    _assert_source_inventory_unchanged,
    _atomic_upload_sources,
    _finish_atomic_publish,
    main,
)
from dsimaging_admin.s3 import list_objects


class DatasetCliTests(unittest.TestCase):
    def test_publish_help_does_not_advertise_unreachable_skip_mode(self):
        result = CliRunner().invoke(main, ["dataset", "publish", "--help"])

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertNotIn("--no-skip", result.output)

    def test_non_atomic_publication_is_disabled(self):
        result = CliRunner().invoke(main, [
            "dataset", "publish", "--dataset-id", "study",
            "--source", ".", "--privacy-unit-column", "patient_id",
            "--no-atomic",
        ])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("non-atomic dataset publishing is disabled", result.output)

    def test_dataset_group_list_and_top_level_alias(self):
        runner = CliRunner()
        with patch("dsimaging_admin.cli.list_datasets", return_value=[]):
            grouped = runner.invoke(main, ["dataset", "list"])
            self.assertEqual(grouped.exit_code, 0, grouped.output)
            self.assertIn("No datasets found.", grouped.output)
            self.assertNotIn("deprecated", grouped.stderr)

            alias = runner.invoke(main, ["list"])
            self.assertEqual(alias.exit_code, 0, alias.output)
            self.assertIn("No datasets found.", alias.output)
            self.assertIn(
                "dsimaging-admin list: deprecated; "
                "use 'dsimaging-admin dataset list' instead",
                alias.stderr,
            )

    @unittest.skipUnless(HAS_MOTO, "moto is not installed")
    def test_atomic_publish_does_not_overwrite_an_active_lock(self):
        with mock_aws(), tempfile.TemporaryDirectory() as tmpdir:
            bucket = "imaging-data"
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=bucket)
            lock_key = "datasets/study/.publish-lock"
            s3.put_object(Bucket=bucket, Key=lock_key, Body=b"first-writer")
            source = self._make_source(tmpdir, "case001", b"nifti")
            metadata = self._make_metadata(
                tmpdir, [("case001", "patient-a")], "metadata.csv"
            )

            result = CliRunner().invoke(main, [
                "--backend", "aws", "--bucket", bucket,
                "dataset", "publish", "--dataset-id", "study",
                "--source", source, "--metadata", metadata,
                "--privacy-unit-column", "patient_id",
                "--skip-dicom-checks", "--replace",
            ])

            self.assertNotEqual(result.exit_code, 0)
            self.assertIn("active atomic publication", str(result.exception))
            self.assertEqual(
                s3.get_object(Bucket=bucket, Key=lock_key)["Body"].read(),
                b"first-writer",
            )
            objects = s3.list_objects_v2(
                Bucket=bucket, Prefix="datasets/study/.staging-"
            ).get("Contents", [])
            self.assertEqual(objects, [])

    @unittest.skipUnless(HAS_MOTO, "moto is not installed")
    def test_atomic_finish_does_not_remove_a_replacement_lock(self):
        with mock_aws():
            bucket = "imaging-data"
            prefix = "datasets/study"
            lock_key = f"{prefix}/.publish-lock"
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=bucket)

            transaction = _atomic_upload_sources(
                s3, bucket, prefix, [], [], require_empty=True)
            replacement = b'{"status":"publishing","owner":"replacement"}'
            s3.put_object(Bucket=bucket, Key=lock_key, Body=replacement)

            finished = _finish_atomic_publish(
                s3, bucket, prefix, transaction, commit=True)

            self.assertFalse(finished)
            self.assertEqual(
                s3.get_object(Bucket=bucket, Key=lock_key)["Body"].read(),
                replacement,
            )

    @unittest.skipUnless(HAS_MOTO, "moto is not installed")
    def test_publish_records_immutable_version_for_quick_verify(self):
        with mock_aws(), tempfile.TemporaryDirectory() as tmpdir:
            bucket = "imaging-data"
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=bucket)
            s3.put_bucket_versioning(
                Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
            runner = CliRunner()
            self._publish_one(
                runner, bucket, tmpdir, "study", "case001", "patient-a",
                "versioned",
            )

            result = runner.invoke(main, [
                "--backend", "aws", "--bucket", bucket,
                "dataset", "verify", "study", "--quick", "--output", "json",
            ])

            self.assertEqual(result.exit_code, 0, result.output)
            self.assertEqual(json.loads(result.output)["quick_ok"], 1)

    @unittest.skipUnless(HAS_MOTO, "moto is not installed")
    def test_publish_rolls_back_if_source_version_changes_before_manifest(self):
        with mock_aws(), tempfile.TemporaryDirectory() as tmpdir:
            bucket = "imaging-data"
            prefix = "datasets/study"
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=bucket)
            s3.put_bucket_versioning(
                Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
            source = self._make_source(tmpdir, "case001", b"image")
            metadata = self._make_metadata(
                tmpdir, [("case001", "patient-a")], "metadata.csv")

            with patch(
                "dsimaging_admin.cli._assert_source_inventory_unchanged",
                side_effect=self._replace_source_with_identical_new_version,
            ):
                result = CliRunner().invoke(main, [
                    "--backend", "aws", "--bucket", bucket,
                    "dataset", "publish", "--dataset-id", "study",
                    "--source", source, "--metadata", metadata,
                    "--privacy-unit-column", "patient_id",
                    "--skip-dicom-checks",
                ])

            self.assertNotEqual(result.exit_code, 0)
            self.assertIn("source inventory changed", str(result.exception))
            self.assertEqual(
                s3.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}/").get(
                    "Contents", []),
                [],
            )

    @unittest.skipUnless(HAS_MOTO, "moto is not installed")
    def test_label_levels_survive_publish_copy_rescan_and_verify(self):
        with mock_aws(), tempfile.TemporaryDirectory() as tmpdir:
            bucket = "imaging-data"
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=bucket)
            source = self._make_source(
                tmpdir, "case001", b"nifti", root="label-levels")
            metadata = os.path.join(tmpdir, "labels.csv")
            with open(metadata, "w", encoding="utf-8") as handle:
                handle.write(
                    "sample_id,patient_id,label\ncase001,patient-a,case\n")
            runner = CliRunner()
            publish_result = runner.invoke(main, [
                "--backend", "aws", "--bucket", bucket,
                "dataset", "publish", "--dataset-id", "source",
                "--source", source, "--metadata", metadata,
                "--privacy-unit-column", "patient_id",
                "--label-column", "label",
                "--public-label-level", "case",
                "--public-label-level", "control",
                "--skip-dicom-checks",
            ])
            self.assertEqual(publish_result.exit_code, 0, publish_result.output)

            copy_result = runner.invoke(main, [
                "--backend", "aws", "--bucket", bucket,
                "dataset", "copy", "source", "target", "--yes",
            ])
            self.assertEqual(copy_result.exit_code, 0, copy_result.output)
            rescan_result = runner.invoke(main, [
                "--backend", "aws", "--bucket", bucket,
                "dataset", "rescan", "target",
            ])
            self.assertEqual(rescan_result.exit_code, 0, rescan_result.output)
            verify_result = runner.invoke(main, [
                "--backend", "aws", "--bucket", bucket,
                "dataset", "verify", "target", "--output", "json",
            ])
            self.assertEqual(verify_result.exit_code, 0, verify_result.output)
            self.assertTrue(json.loads(verify_result.output)["ok"])
            manifest = yaml.safe_load(s3.get_object(
                Bucket=bucket, Key="datasets/target/manifest.yaml")["Body"].read())
            self.assertEqual(
                manifest["metadata"]["label_levels"], ["case", "control"])

    @unittest.skipUnless(HAS_MOTO, "moto is not installed")
    def test_publish_failure_restores_exact_previous_dataset(self):
        with mock_aws(), tempfile.TemporaryDirectory() as tmpdir:
            bucket = "imaging-data"
            prefix = "datasets/study"
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=bucket)
            s3.put_object(
                Bucket=bucket, Key=f"{prefix}/manifest.yaml", Body=b"old-manifest")
            s3.put_object(
                Bucket=bucket, Key=f"{prefix}/source/images/old.nii.gz",
                Body=b"old-image")
            before = self._snapshot_prefix(s3, bucket, f"{prefix}/")
            source = self._make_source(tmpdir, "case001", b"new-image")
            metadata = self._make_metadata(
                tmpdir, [("case001", "patient-a")], "metadata.csv")

            with patch(
                "dsimaging_admin.cli._write_dataset_artifacts",
                side_effect=RuntimeError("injected artifact failure"),
            ):
                result = CliRunner().invoke(main, [
                    "--backend", "aws", "--bucket", bucket,
                    "dataset", "publish", "--dataset-id", "study",
                    "--source", source, "--metadata", metadata,
                    "--privacy-unit-column", "patient_id",
                    "--skip-dicom-checks", "--replace",
                ])

            self.assertNotEqual(result.exit_code, 0)
            self.assertIn("injected artifact failure", str(result.exception))
            self.assertEqual(
                self._snapshot_prefix(s3, bucket, f"{prefix}/"), before)

    @unittest.skipUnless(HAS_MOTO, "moto is not installed")
    def test_dataset_modify_metadata_replacement_roundtrip(self):
        with mock_aws(), tempfile.TemporaryDirectory() as tmpdir:
            bucket = "imaging-data"
            boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=bucket)
            source = self._make_source(tmpdir, "case001", b"nifti-one")
            metadata = os.path.join(tmpdir, "metadata.csv")
            with open(metadata, "w", encoding="utf-8") as f:
                f.write("sample_id,patient_id,age\ncase001,patient-a,60\n")

            runner = CliRunner()
            result = runner.invoke(main, [
                "--backend", "aws",
                "--bucket", bucket,
                "dataset", "publish",
                "--dataset-id", "lung_ct_v1",
                "--source", source,
                "--metadata", metadata,
                "--privacy-unit-column", "patient_id",
                "--modality", "ct",
                "--skip-dicom-checks",
            ])
            self.assertEqual(result.exit_code, 0, result.output)

            replacement = os.path.join(tmpdir, "replacement.csv")
            with open(replacement, "w", encoding="utf-8") as f:
                f.write("sample_id,patient_id,age,stage\ncase001,patient-a,61,IIIA\n")
            result = runner.invoke(main, [
                "--backend", "aws",
                "--bucket", bucket,
                "dataset", "modify",
                "lung_ct_v1",
                "--metadata", replacement,
                "--yes",
            ])
            self.assertEqual(result.exit_code, 0, result.output)
            table = self._read_parquet_object(
                bucket, "datasets/lung_ct_v1/metadata/samples.parquet"
            )
            self.assertIn("stage", table.column_names)
            self.assertEqual(table["age"].to_pylist(), [61])
            self.assertEqual(table["stage"].to_pylist(), ["IIIA"])

    @unittest.skipUnless(HAS_MOTO, "moto is not installed")
    def test_backend_aws_ignores_profile_endpoint(self):
        """--backend aws must not inherit the stored profile's MinIO endpoint."""
        with mock_aws(), tempfile.TemporaryDirectory() as tmpdir:
            bucket = "imaging-data"
            boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=bucket)
            source = self._make_source(tmpdir, "case001", b"nifti-one")
            metadata = self._make_metadata(
                tmpdir, [("case001", "patient-a")], "backend-metadata.csv"
            )
            config_path = os.path.join(tmpdir, "dsimaging.yaml")
            with open(config_path, "w", encoding="utf-8") as f:
                f.write(
                    "default_profile: default\n"
                    "profiles:\n"
                    "  default:\n"
                    "    endpoint: http://127.0.0.1:9000\n"
                )

            runner = CliRunner()
            with patch("dsimaging_admin.cli.CONFIG_PATH", config_path):
                result = runner.invoke(main, [
                    "--backend", "aws",
                    "--bucket", bucket,
                    "dataset", "publish",
                    "--dataset-id", "lung_ct_v1",
                    "--source", source,
                    "--metadata", metadata,
                    "--privacy-unit-column", "patient_id",
                    "--modality", "ct",
                    "--skip-dicom-checks",
                ])
            self.assertEqual(result.exit_code, 0, result.output)

    @unittest.skipUnless(HAS_MOTO, "moto is not installed")
    def test_dataset_modify_add_images_and_dry_run(self):
        with mock_aws(), tempfile.TemporaryDirectory() as tmpdir:
            bucket = "imaging-data"
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=bucket)
            source = self._make_source(tmpdir, "case001", b"nifti-one")
            metadata = self._make_metadata(
                tmpdir, [("case001", "patient-a")], "initial-metadata.csv"
            )
            runner = CliRunner()
            result = runner.invoke(main, [
                "--backend", "aws",
                "--bucket", bucket,
                "dataset", "publish",
                "--dataset-id", "lung_ct_v1",
                "--source", source,
                "--metadata", metadata,
                "--privacy-unit-column", "patient_id",
                "--modality", "ct",
                "--skip-dicom-checks",
            ])
            self.assertEqual(result.exit_code, 0, result.output)

            add_dir = self._make_source(tmpdir, "case002", b"nifti-two", root="add")
            result = runner.invoke(main, [
                "--backend", "aws",
                "--bucket", bucket,
                "dataset", "modify",
                "lung_ct_v1",
                "--add-images", add_dir,
                "--dry-run",
            ])
            self.assertEqual(result.exit_code, 0, result.output)
            self.assertIn("Dry run complete", result.output)
            self.assertFalse(self._object_exists(
                s3, bucket, "datasets/lung_ct_v1/source/images/case002.nii.gz"
            ))

            replacement = self._make_metadata(
                tmpdir,
                [("case001", "patient-a"), ("case002", "patient-a")],
                "two-sample-metadata.csv",
            )
            result = runner.invoke(main, [
                "--backend", "aws",
                "--bucket", bucket,
                "dataset", "modify",
                "lung_ct_v1",
                "--add-images", add_dir,
                "--metadata", replacement,
                "--yes",
            ])
            self.assertEqual(result.exit_code, 0, result.output)
            table = self._read_parquet_object(
                bucket, "datasets/lung_ct_v1/indexes/content_hash_index.parquet"
            )
            self.assertEqual(sorted(table["sample_id"].to_pylist()), ["case001", "case002"])

            masks_dir = self._make_masks(tmpdir, "case001", b"mask-one")
            result = runner.invoke(main, [
                "--backend", "aws",
                "--bucket", bucket,
                "dataset", "modify",
                "lung_ct_v1",
                "--add-masks", masks_dir,
            ])
            self.assertEqual(result.exit_code, 0, result.output)
            table = self._read_parquet_object(
                bucket, "datasets/lung_ct_v1/indexes/masks_content_hash_index.parquet"
            )
            self.assertEqual(table["sample_id"].to_pylist(), ["case001"])

    @unittest.skipUnless(HAS_MOTO, "moto is not installed")
    def test_publish_requires_explicit_replace_and_removes_historical_sources(self):
        with mock_aws(), tempfile.TemporaryDirectory() as tmpdir:
            bucket = "imaging-data"
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=bucket)
            runner = CliRunner()
            first_source = self._make_source(tmpdir, "case001", b"first", root="first")
            first_metadata = self._make_metadata(
                tmpdir, [("case001", "patient-a")], "first.csv"
            )
            base_args = [
                "--backend", "aws", "--bucket", bucket,
                "dataset", "publish", "--dataset-id", "study",
                "--privacy-unit-column", "patient_id", "--skip-dicom-checks",
            ]
            result = runner.invoke(main, base_args + [
                "--source", first_source, "--metadata", first_metadata,
            ])
            self.assertEqual(result.exit_code, 0, result.output)

            second_source = self._make_source(tmpdir, "case002", b"second", root="second")
            second_metadata = self._make_metadata(
                tmpdir, [("case002", "patient-b")], "second.csv"
            )
            result = runner.invoke(main, base_args + [
                "--source", second_source, "--metadata", second_metadata,
            ])
            self.assertNotEqual(result.exit_code, 0)
            self.assertIn("use --replace", result.output)

            result = runner.invoke(main, base_args + [
                "--source", second_source, "--metadata", second_metadata,
                "--replace",
            ])
            self.assertEqual(result.exit_code, 0, result.output)
            self.assertFalse(self._object_exists(
                s3, bucket, "datasets/study/source/images/case001.nii.gz"
            ))
            self.assertTrue(self._object_exists(
                s3, bucket, "datasets/study/source/images/case002.nii.gz"
            ))
            manifest = yaml.safe_load(
                s3.get_object(Bucket=bucket, Key="datasets/study/manifest.yaml")["Body"].read()
            )
            self.assertEqual(manifest["metadata"]["privacy_unit_col"], "patient_id")

    @unittest.skipUnless(HAS_MOTO, "moto is not installed")
    def test_modify_failure_restores_sources_and_derived_objects(self):
        with mock_aws(), tempfile.TemporaryDirectory() as tmpdir:
            bucket = "imaging-data"
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=bucket)
            runner = CliRunner()
            self._publish_one(
                runner, bucket, tmpdir, "study", "case001", "patient-a",
                "initial",
            )
            before = self._snapshot_prefix(s3, bucket, "datasets/study/")
            added = self._make_source(
                tmpdir, "case002", b"second", root="added")
            metadata = self._make_metadata(
                tmpdir,
                [("case001", "patient-a"), ("case002", "patient-b")],
                "modified.csv",
            )

            with patch(
                "dsimaging_admin.cli._write_dataset_artifacts",
                side_effect=RuntimeError("injected modify failure"),
            ):
                result = runner.invoke(main, [
                    "--backend", "aws", "--bucket", bucket,
                    "dataset", "modify", "study",
                    "--add-images", added, "--metadata", metadata, "--yes",
                ])

            self.assertNotEqual(result.exit_code, 0)
            self.assertEqual(
                self._snapshot_prefix(s3, bucket, "datasets/study/"), before)

    @unittest.skipUnless(HAS_MOTO, "moto is not installed")
    def test_rescan_rejects_roster_drift_without_reducing_metadata(self):
        with mock_aws(), tempfile.TemporaryDirectory() as tmpdir:
            bucket = "imaging-data"
            prefix = "datasets/study"
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=bucket)
            runner = CliRunner()
            self._publish_one(
                runner, bucket, tmpdir, "study", "case001", "patient-a",
                "initial",
            )
            s3.put_object(
                Bucket=bucket,
                Key=f"{prefix}/source/images/case002.nii.gz",
                Body=b"unmatched-image",
            )
            before = self._snapshot_prefix(s3, bucket, f"{prefix}/")

            result = runner.invoke(main, [
                "--backend", "aws", "--bucket", bucket,
                "dataset", "rescan", "study",
            ])

            self.assertNotEqual(result.exit_code, 0)
            self.assertIn("roster must exactly match", str(result.exception))
            self.assertEqual(
                self._snapshot_prefix(s3, bucket, f"{prefix}/"), before)

    @unittest.skipUnless(HAS_MOTO, "moto is not installed")
    def test_rescan_restores_dataset_if_source_version_changes_before_manifest(self):
        with mock_aws(), tempfile.TemporaryDirectory() as tmpdir:
            bucket = "imaging-data"
            prefix = "datasets/study"
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=bucket)
            s3.put_bucket_versioning(
                Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
            runner = CliRunner()
            self._publish_one(
                runner, bucket, tmpdir, "study", "case001", "patient-a",
                "initial",
            )
            before = self._snapshot_prefix(s3, bucket, f"{prefix}/")

            with patch(
                "dsimaging_admin.cli._assert_source_inventory_unchanged",
                side_effect=self._replace_source_with_identical_new_version,
            ):
                result = runner.invoke(main, [
                    "--backend", "aws", "--bucket", bucket,
                    "dataset", "rescan", "study",
                ])

            self.assertNotEqual(result.exit_code, 0)
            self.assertIn("source inventory changed", str(result.exception))
            self.assertEqual(
                self._snapshot_prefix(s3, bucket, f"{prefix}/"), before)

    @unittest.skipUnless(HAS_MOTO, "moto is not installed")
    def test_copy_failure_restores_destination_and_releases_source_lock(self):
        with mock_aws(), tempfile.TemporaryDirectory() as tmpdir:
            bucket = "imaging-data"
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=bucket)
            runner = CliRunner()
            self._publish_one(
                runner, bucket, tmpdir, "source", "case001", "patient-a",
                "source-files",
            )
            self._publish_one(
                runner, bucket, tmpdir, "target", "old001", "patient-old",
                "target-files",
            )
            source_before = self._snapshot_prefix(
                s3, bucket, "datasets/source/")
            target_before = self._snapshot_prefix(
                s3, bucket, "datasets/target/")

            with patch(
                "dsimaging_admin.cli._write_dataset_artifacts",
                side_effect=RuntimeError("injected copy failure"),
            ):
                result = runner.invoke(main, [
                    "--backend", "aws", "--bucket", bucket,
                    "dataset", "copy", "source", "target",
                    "--yes", "--replace",
                ])

            self.assertNotEqual(result.exit_code, 0)
            self.assertEqual(
                self._snapshot_prefix(s3, bucket, "datasets/source/"),
                source_before,
            )
            self.assertEqual(
                self._snapshot_prefix(s3, bucket, "datasets/target/"),
                target_before,
            )

    @unittest.skipUnless(HAS_MOTO, "moto is not installed")
    def test_copy_removes_destination_if_source_version_changes_before_manifest(self):
        with mock_aws(), tempfile.TemporaryDirectory() as tmpdir:
            bucket = "imaging-data"
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=bucket)
            s3.put_bucket_versioning(
                Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
            runner = CliRunner()
            self._publish_one(
                runner, bucket, tmpdir, "source", "case001", "patient-a",
                "source-files",
            )
            source_before = self._snapshot_prefix(
                s3, bucket, "datasets/source/")

            with patch(
                "dsimaging_admin.cli._assert_source_inventory_unchanged",
                side_effect=self._replace_source_with_identical_new_version,
            ):
                result = runner.invoke(main, [
                    "--backend", "aws", "--bucket", bucket,
                    "dataset", "copy", "source", "target", "--yes",
                ])

            self.assertNotEqual(result.exit_code, 0)
            self.assertIn("source inventory changed", str(result.exception))
            self.assertEqual(
                self._snapshot_prefix(s3, bucket, "datasets/source/"),
                source_before,
            )
            self.assertEqual(
                s3.list_objects_v2(
                    Bucket=bucket, Prefix="datasets/target/"
                ).get("Contents", []),
                [],
            )

    @unittest.skipUnless(HAS_MOTO, "moto is not installed")
    def test_source_inventory_recheck_detects_dicom_slice_version_change(self):
        with mock_aws():
            bucket = "imaging-data"
            prefix = "datasets/study"
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=bucket)
            s3.put_bucket_versioning(
                Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
            for filename in ("001.dcm", "002.dcm"):
                s3.put_object(
                    Bucket=bucket,
                    Key=f"{prefix}/source/images/series1/{filename}",
                    Body=filename.encode("utf-8"),
                )
            expected_images = list_objects(
                s3, bucket, f"{prefix}/source/images/",
                include_version_ids=True,
            )

            with self.assertRaisesRegex(RuntimeError, "source inventory changed"):
                self._replace_source_with_identical_new_version(
                    s3, bucket, prefix, expected_images, [])

    @unittest.skipUnless(HAS_MOTO, "moto is not installed")
    def test_copy_defers_to_an_active_source_lock(self):
        with mock_aws(), tempfile.TemporaryDirectory() as tmpdir:
            bucket = "imaging-data"
            prefix = "datasets/source"
            lock_key = f"{prefix}/.publish-lock"
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=bucket)
            runner = CliRunner()
            self._publish_one(
                runner, bucket, tmpdir, "source", "case001", "patient-a",
                "source-files",
            )
            lock = b'{"status":"publishing","owner":"other"}'
            s3.put_object(Bucket=bucket, Key=lock_key, Body=lock)

            result = runner.invoke(main, [
                "--backend", "aws", "--bucket", bucket,
                "dataset", "copy", "source", "target", "--yes",
            ])

            self.assertNotEqual(result.exit_code, 0)
            self.assertIn("active publication", result.output)
            self.assertEqual(
                s3.get_object(Bucket=bucket, Key=lock_key)["Body"].read(), lock)
            self.assertEqual(
                s3.list_objects_v2(
                    Bucket=bucket, Prefix="datasets/target/"
                ).get("Contents", []),
                [],
            )

    @unittest.skipUnless(HAS_MOTO, "moto is not installed")
    def test_download_rejects_object_paths_outside_destination(self):
        with mock_aws(), tempfile.TemporaryDirectory() as tmpdir:
            bucket = "imaging-data"
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=bucket)
            s3.put_object(
                Bucket=bucket, Key="datasets/study/../outside.txt",
                Body=b"must-not-be-written",
            )
            destination = os.path.join(tmpdir, "download")

            result = CliRunner().invoke(main, [
                "--backend", "aws", "--bucket", bucket,
                "dataset", "download", "study", destination,
            ])

            self.assertNotEqual(result.exit_code, 0)
            self.assertIn("unsafe object path", result.output)
            self.assertFalse(os.path.exists(os.path.join(tmpdir, "outside.txt")))

    def _make_source(self, tmpdir: str, sample_id: str, payload: bytes,
                     root: str = "source") -> str:
        source = os.path.join(tmpdir, root)
        images = os.path.join(source, "images")
        os.makedirs(images, exist_ok=True)
        with open(os.path.join(images, f"{sample_id}.nii.gz"), "wb") as f:
            f.write(payload)
        return source

    def _make_masks(self, tmpdir: str, sample_id: str, payload: bytes) -> str:
        source = os.path.join(tmpdir, "mask_add")
        masks = os.path.join(source, "masks")
        os.makedirs(masks, exist_ok=True)
        with open(os.path.join(masks, f"{sample_id}_mask.nii.gz"), "wb") as f:
            f.write(payload)
        return source

    def _make_metadata(self, tmpdir: str, rows: list[tuple[str, str]],
                       filename: str) -> str:
        path = os.path.join(tmpdir, filename)
        with open(path, "w", encoding="utf-8") as f:
            f.write("sample_id,patient_id\n")
            for sample_id, patient_id in rows:
                f.write(f"{sample_id},{patient_id}\n")
        return path

    def _publish_one(self, runner: CliRunner, bucket: str, tmpdir: str,
                     dataset_id: str, sample_id: str, patient_id: str,
                     root: str) -> None:
        source = self._make_source(
            tmpdir, sample_id, sample_id.encode("utf-8"), root=root)
        metadata = self._make_metadata(
            tmpdir, [(sample_id, patient_id)], f"{dataset_id}.csv")
        result = runner.invoke(main, [
            "--backend", "aws", "--bucket", bucket,
            "dataset", "publish", "--dataset-id", dataset_id,
            "--source", source, "--metadata", metadata,
            "--privacy-unit-column", "patient_id", "--skip-dicom-checks",
        ])
        self.assertEqual(result.exit_code, 0, result.output)

    def _read_parquet_object(self, bucket: str, key: str) -> pa.Table:
        body = boto3.client("s3", region_name="us-east-1").get_object(
            Bucket=bucket, Key=key
        )["Body"]
        try:
            return pq.read_table(pa.BufferReader(body.read()))
        finally:
            body.close()

    def _object_exists(self, s3, bucket: str, key: str) -> bool:
        try:
            s3.head_object(Bucket=bucket, Key=key)
            return True
        except Exception:
            return False

    def _snapshot_prefix(self, s3, bucket: str, prefix: str) -> dict[str, bytes]:
        objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get(
            "Contents", [])
        return {
            item["Key"]: s3.get_object(Bucket=bucket, Key=item["Key"])[
                "Body"].read()
            for item in objects
        }

    def _replace_source_with_identical_new_version(
        self, s3, bucket: str, prefix: str,
        expected_images: list[dict], expected_masks: list[dict],
    ) -> None:
        key = expected_images[-1]["key"]
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        s3.put_object(Bucket=bucket, Key=key, Body=body)
        _assert_source_inventory_unchanged(
            s3, bucket, prefix, expected_images, expected_masks)


if __name__ == "__main__":
    unittest.main()
