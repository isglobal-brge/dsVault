import datetime as dt
import io
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

try:
    import boto3
    from moto import mock_aws
    HAS_MOTO = True
except Exception:
    boto3 = None
    mock_aws = None
    HAS_MOTO = False


try:
    from streamlit.testing.v1 import AppTest
    HAS_STREAMLIT = True
except Exception:
    AppTest = None
    HAS_STREAMLIT = False


ROOT = Path(__file__).resolve().parents[1]
UI_PATH = ROOT / "src" / "dsimaging_admin" / "ui.py"
FIXTURE = ROOT / "tests" / "fixtures" / "tiny_dataset"


class FakeBody(io.BytesIO):
    pass


class FakePaginator:
    def __init__(self, objects):
        self.objects = objects

    def paginate(self, Bucket, Prefix, Delimiter=None):
        contents = []
        prefixes = set()
        for key, value in self.objects.items():
            if not key.startswith(Prefix):
                continue
            suffix = key[len(Prefix):]
            if Delimiter and Delimiter in suffix:
                prefixes.add(Prefix + suffix.split(Delimiter, 1)[0] + Delimiter)
                continue
            contents.append({
                "Key": key,
                "Size": len(value["body"]),
                "LastModified": value["last_modified"],
                "ETag": f'"{value.get("etag", "etag")}"',
            })
        page = {"Contents": contents}
        if prefixes:
            page["CommonPrefixes"] = [{"Prefix": prefix} for prefix in sorted(prefixes)]
        return [page]


class FakeS3:
    def __init__(self):
        now = dt.datetime(2026, 5, 25, 10, 0, tzinfo=dt.timezone.utc)
        samples = pa.table({
            "sample_id": ["case001"],
            "source_kind": ["single_file"],
            "n_files": pa.array([1], type=pa.int32()),
        })
        index = pa.table({
            "sample_id": ["case001"],
            "uri": ["s3://imaging-data/datasets/lung_ct_v1/source/images/case001.nii.gz"],
            "content_hash": ["abc"],
            "size": pa.array([12], type=pa.int64()),
            "last_modified": ["2026-05-25T10:00:00Z"],
            "version_id": pa.array([None], type=pa.string()),
            "etag": pa.array(["etag"], type=pa.string()),
            "source_kind": ["single_file"],
        })
        self.objects = {
            "datasets/lung_ct_v1/manifest.yaml": {
                "body": yaml.safe_dump({
                    "schema_version": 1,
                    "dataset_id": "lung_ct_v1",
                    "modality": "ct",
                }).encode(),
                "last_modified": now,
            },
            "datasets/lung_ct_v1/source/images/case001.nii.gz": {
                "body": b"image",
                "last_modified": now,
            },
            "datasets/lung_ct_v1/metadata/samples.parquet": {
                "body": parquet_bytes(samples),
                "last_modified": now,
            },
            "datasets/lung_ct_v1/indexes/content_hash_index.parquet": {
                "body": parquet_bytes(index),
                "last_modified": now,
            },
        }

    def list_buckets(self):
        return {"Buckets": [{"Name": "imaging-data"}]}

    def head_bucket(self, Bucket):
        return {}

    def get_bucket_versioning(self, Bucket):
        return {"Status": "Enabled"}

    def get_bucket_encryption(self, Bucket):
        return {"ServerSideEncryptionConfiguration": {"Rules": []}}

    def get_bucket_notification_configuration(self, Bucket):
        return {"QueueConfigurations": [{"QueueArn": "arn:queue"}]}

    def get_paginator(self, name):
        return FakePaginator(self.objects)

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise KeyError(Key)
        item = self.objects[Key]
        return {
            "ContentLength": len(item["body"]),
            "LastModified": item["last_modified"],
            "ETag": '"etag"',
        }

    def get_object(self, Bucket, Key):
        return {"Body": FakeBody(self.objects[Key]["body"])}

    def delete_objects(self, Bucket, Delete):
        for item in Delete["Objects"]:
            self.objects.pop(item["Key"], None)
        return {}


def parquet_bytes(table):
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    return sink.getvalue().to_pybytes()


def rendered_text(at):
    chunks = []
    for collection in (
        at.title,
        at.header,
        at.subheader,
        at.caption,
        at.markdown,
        at.code,
    ):
        chunks.extend(getattr(item, "value", "") for item in collection)
    chunks.extend(item.value for item in at.selectbox)
    chunks.extend(item.label for item in at.metric)
    chunks.extend(item.value for item in at.metric)
    return "\n".join(str(chunk) for chunk in chunks)


class UISourceTests(unittest.TestCase):
    def test_ui_source_has_no_resource_registry_coupling(self):
        text = UI_PATH.read_text(encoding="utf-8").lower()
        self.assertNotIn("opal", text)
        self.assertNotIn("urllib", text)

    @unittest.skipUnless(HAS_STREAMLIT, "streamlit testing extra is not installed")
    def test_ui_publish_reports_dicom_warnings(self):
        from dsimaging_admin import ui

        logs = []
        with patch.object(ui, "make_s3_client", return_value=object()), \
                patch.object(ui, "list_objects", return_value=[]), \
                patch.object(ui, "scan_images", return_value=[{"sample_id": "case001"}]), \
                patch.object(ui, "validate_dicom_series", return_value=["bad series"]), \
                patch.object(ui, "scan_masks", return_value=[]), \
                patch.object(
                    ui, "build_samples_metadata",
                    side_effect=RuntimeError("stop before upload")
                ) as metadata_builder:
            with self.assertRaisesRegex(RuntimeError, "stop before upload"):
                ui.publish_dataset(
                    {"bucket": "imaging-data"}, "study", "/source", "",
                    "patient_id", None, "ct", False, None, logs.append,
                    label_levels=["case"],
                )

        self.assertIn("WARN: bad series", logs)
        self.assertEqual(
            metadata_builder.call_args.kwargs["label_levels"], ["case"])


@unittest.skipUnless(HAS_STREAMLIT, "streamlit testing extra is not installed")
class StreamlitUITests(unittest.TestCase):
    def setUp(self):
        self.old_config = os.environ.get("DSIMAGING_CONFIG")
        self.tmp = tempfile.TemporaryDirectory()
        self.config_path = os.path.join(self.tmp.name, "config.yaml")
        with open(self.config_path, "w", encoding="utf-8") as f:
            yaml.safe_dump({
                "default_profile": "server_1",
                "profiles": {
                    "server_1": {
                        "endpoint": "http://127.0.0.1:9000",
                        "bucket": "imaging-data",
                        "access_key": "secret-access",
                        "secret_key": "secret-key",
                        "region": "",
                        "controller_url": "http://127.0.0.1:8080",
                    }
                }
            }, f)
        os.environ["DSIMAGING_CONFIG"] = self.config_path

    def tearDown(self):
        if self.old_config is None:
            os.environ.pop("DSIMAGING_CONFIG", None)
        else:
            os.environ["DSIMAGING_CONFIG"] = self.old_config
        self.tmp.cleanup()

    def app(self):
        at = AppTest.from_file(str(UI_PATH))
        at.session_state["_s3_client"] = FakeS3()
        return at.run()

    def test_profile_picker_loads_config(self):
        at = self.app()
        self.assertIn("server_1", [item.value for item in at.selectbox])

    def test_doctor_checks_render_state_pills(self):
        at = self.app()
        [button for button in at.button if button.label == "Run doctor"][0].click().run()
        text = rendered_text(at)
        self.assertIn("endpoint reachable", text)
        self.assertIn("`ok`", text)

    def test_datasets_list_populates(self):
        at = self.app()
        at.radio[0].set_value("Datasets").run()
        self.assertIn("lung_ct_v1", rendered_text(at))

    def test_publish_preview_scan_counts_fixture(self):
        at = self.app()
        at.radio[0].set_value("Publish").run()
        at.text_input(key="publish-source").set_value(str(FIXTURE)).run()
        [button for button in at.button if button.label == "Preview scan"][0].click().run()
        text = rendered_text(at)
        self.assertIn("case001", text)
        self.assertIn("Images", text)

    def test_delete_confirmation_gate(self):
        at = self.app()
        at.radio[0].set_value("Delete").run()
        delete_button = [button for button in at.button if button.label == "Delete dataset"][0]
        self.assertTrue(delete_button.disabled)
        [
            field for field in at.text_input
            if field.label == "Type the dataset ID to confirm"
        ][0].set_value("lung_ct_v1").run()
        delete_button = [button for button in at.button if button.label == "Delete dataset"][0]
        self.assertFalse(delete_button.disabled)

    @unittest.skipUnless(HAS_MOTO, "moto is not installed")
    def test_delete_preserves_versions_unless_purge_is_selected(self):
        for purge_versions in (False, True):
            with self.subTest(purge_versions=purge_versions), mock_aws():
                s3 = boto3.client(
                    "s3",
                    region_name="us-east-1",
                    aws_access_key_id="testing",
                    aws_secret_access_key="testing",
                )
                s3.create_bucket(Bucket="imaging-data")
                s3.put_bucket_versioning(
                    Bucket="imaging-data",
                    VersioningConfiguration={"Status": "Enabled"},
                )
                key = "datasets/versioned_ct_v1/manifest.yaml"
                for modality in ("ct", "mr"):
                    s3.put_object(
                        Bucket="imaging-data",
                        Key=key,
                        Body=yaml.safe_dump({
                            "schema_version": 1,
                            "dataset_id": "versioned_ct_v1",
                            "modality": modality,
                        }).encode(),
                    )

                at = AppTest.from_file(str(UI_PATH))
                at.session_state["_s3_client"] = s3
                at.run()
                at.radio[0].set_value("Delete").run()
                [
                    checkbox for checkbox in at.checkbox
                    if checkbox.label == "Dry run"
                ][0].set_value(False).run()
                purge_checkbox = [
                    checkbox for checkbox in at.checkbox
                    if checkbox.label == "Purge all object versions and delete markers"
                ][0]
                self.assertFalse(purge_checkbox.value)
                if purge_versions:
                    purge_checkbox.set_value(True).run()
                [
                    field for field in at.text_input
                    if field.label == "Type the dataset ID to confirm"
                ][0].set_value("versioned_ct_v1").run()
                [
                    button for button in at.button
                    if button.label == "Delete dataset"
                ][0].click().run()

                history = s3.list_object_versions(
                    Bucket="imaging-data", Prefix="datasets/versioned_ct_v1/"
                )
                remaining = (
                    history.get("Versions", []) + history.get("DeleteMarkers", [])
                )
                self.assertEqual(bool(remaining), not purge_versions)

    def test_secret_inputs_do_not_echo_to_outputs(self):
        at = self.app()
        text = rendered_text(at)
        self.assertNotIn("secret-access", text)
        self.assertNotIn("secret-key", text)
        secret_fields = {
            field.label: field.value
            for field in at.text_input
            if field.label in {
                "Access key", "Secret key", "Controller operator token"
            }
        }
        self.assertEqual(secret_fields, {
            "Access key": "",
            "Secret key": "",
            "Controller operator token": "",
        })
        self.assertIn("not set", text)

    @unittest.skipUnless(HAS_MOTO, "moto is not installed")
    def test_datasets_tab_with_moto_s3(self):
        with mock_aws():
            s3 = boto3.client(
                "s3",
                region_name="us-east-1",
                aws_access_key_id="testing",
                aws_secret_access_key="testing",
            )
            s3.create_bucket(Bucket="imaging-data")
            samples = pa.table({
                "sample_id": ["case001"],
                "source_kind": ["single_file"],
                "n_files": pa.array([1], type=pa.int32()),
            })
            index = pa.table({
                "sample_id": ["case001"],
                "uri": [
                    "s3://imaging-data/datasets/moto_ct_v1/"
                    "source/images/case001.nii.gz"
                ],
                "content_hash": ["abc"],
                "size": pa.array([5], type=pa.int64()),
                "last_modified": ["2026-05-25T10:00:00Z"],
                "version_id": pa.array([None], type=pa.string()),
                "etag": pa.array(["etag"], type=pa.string()),
                "source_kind": ["single_file"],
            })
            s3.put_object(
                Bucket="imaging-data",
                Key="datasets/moto_ct_v1/manifest.yaml",
                Body=yaml.safe_dump({
                    "schema_version": 1,
                    "dataset_id": "moto_ct_v1",
                    "modality": "ct",
                }).encode(),
            )
            s3.put_object(
                Bucket="imaging-data",
                Key="datasets/moto_ct_v1/source/images/case001.nii.gz",
                Body=b"image",
            )
            s3.put_object(
                Bucket="imaging-data",
                Key="datasets/moto_ct_v1/metadata/samples.parquet",
                Body=parquet_bytes(samples),
            )
            s3.put_object(
                Bucket="imaging-data",
                Key="datasets/moto_ct_v1/indexes/content_hash_index.parquet",
                Body=parquet_bytes(index),
            )
            at = AppTest.from_file(str(UI_PATH))
            at.session_state["_s3_client"] = s3
            at.run()
            at.radio[0].set_value("Datasets").run()
            self.assertIn("moto_ct_v1", rendered_text(at))


if __name__ == "__main__":
    unittest.main()
