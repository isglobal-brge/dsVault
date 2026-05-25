import os
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from click.testing import CliRunner

try:
    import boto3
    from moto import mock_aws
    HAS_MOTO = True
except Exception:
    boto3 = None
    mock_aws = None
    HAS_MOTO = False

from dsimaging_admin.cli import main


class DatasetCliTests(unittest.TestCase):
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
    def test_dataset_modify_metadata_replacement_roundtrip(self):
        with mock_aws(), tempfile.TemporaryDirectory() as tmpdir:
            bucket = "imaging-data"
            boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=bucket)
            source = self._make_source(tmpdir, "case001", b"nifti-one")
            metadata = os.path.join(tmpdir, "metadata.csv")
            with open(metadata, "w", encoding="utf-8") as f:
                f.write("sample_id,age\ncase001,60\n")

            runner = CliRunner()
            result = runner.invoke(main, [
                "--backend", "aws",
                "--bucket", bucket,
                "dataset", "publish",
                "--dataset-id", "lung_ct_v1",
                "--source", source,
                "--metadata", metadata,
                "--modality", "ct",
                "--no-atomic",
                "--skip-dicom-checks",
            ])
            self.assertEqual(result.exit_code, 0, result.output)

            replacement = os.path.join(tmpdir, "replacement.csv")
            with open(replacement, "w", encoding="utf-8") as f:
                f.write("sample_id,age,stage\ncase001,61,IIIA\n")
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
    def test_dataset_modify_add_images_and_dry_run(self):
        with mock_aws(), tempfile.TemporaryDirectory() as tmpdir:
            bucket = "imaging-data"
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=bucket)
            source = self._make_source(tmpdir, "case001", b"nifti-one")
            runner = CliRunner()
            result = runner.invoke(main, [
                "--backend", "aws",
                "--bucket", bucket,
                "dataset", "publish",
                "--dataset-id", "lung_ct_v1",
                "--source", source,
                "--modality", "ct",
                "--no-atomic",
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

            result = runner.invoke(main, [
                "--backend", "aws",
                "--bucket", bucket,
                "dataset", "modify",
                "lung_ct_v1",
                "--add-images", add_dir,
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


if __name__ == "__main__":
    unittest.main()
