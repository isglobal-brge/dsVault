import datetime as dt
import io
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

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


@unittest.skipUnless(HAS_STREAMLIT, "streamlit testing extra is not installed")
class UIHelperTests(unittest.TestCase):
    def test_local_compose_controls_require_an_owned_project(self):
        import dsimaging_admin.ui as ui

        self.assertFalse(ui._has_owned_local_compose_project({
            "endpoint": "http://127.0.0.1:9000",
        }))
        self.assertFalse(ui._has_owned_local_compose_project({
            "kind": "local-store",
            "store_path": "/tmp/store",
            "endpoint": "https://minio.example.org",
        }))
        self.assertTrue(ui._has_owned_local_compose_project({
            "kind": "local-store",
            "store_path": "/tmp/store",
            "endpoint": "http://127.0.0.1:9000",
        }))

    def test_first_local_project_can_be_initialized_without_an_owned_profile(self):
        import dsimaging_admin.ui as ui

        self.assertTrue(ui._can_initialize_local_project({
            "resolved_backend": "minio",
            "endpoint": "http://127.0.0.1:9000",
        }))
        self.assertFalse(ui._can_initialize_local_project({
            "resolved_backend": "s3-compatible",
            "endpoint": "http://127.0.0.1:9000",
        }))

    def test_compose_path_is_derived_only_from_the_owned_profile(self):
        import dsimaging_admin.ui as ui

        with tempfile.TemporaryDirectory() as tmpdir:
            configured = os.path.join(tmpdir, "store", "..", "store")
            config = {
                "kind": "local-store",
                "store_path": configured,
                "endpoint": "http://127.0.0.1:9000",
            }
            self.assertEqual(
                ui._owned_local_compose_path(config),
                str(Path(configured).resolve()),
            )
        self.assertIsNone(ui._owned_local_compose_path({
            "store_path": "/tmp/unowned",
            "endpoint": "http://127.0.0.1:9000",
        }))

    def test_connection_change_clears_derived_results(self):
        import dsimaging_admin.ui as ui

        state = {
            "_connection_scope": ("old",),
            "doctor_checks": [{"state": "ok"}],
            "compose_status": "healthy",
            "delete-dataset": "study",
            "delete-dry-run": False,
            "delete-purge": True,
            "delete-confirmation": "study",
            "publish_preview_signature": "old-preview",
            "store-project-path": "/tmp/store-a",
            "store-bucket": "bucket-a",
            "store-region": "eu-west-1",
            "store-kms-key": "arn:key-a",
            "modify-dataset": "study",
            "modify-metadata": "/tmp/metadata.csv",
            "modify-images": "/tmp/images",
            "modify-masks": "/tmp/masks",
        }
        config = {
            "_profile": "new",
            "resolved_backend": "aws",
            "endpoint": "",
            "bucket": "imaging-data",
            "controller_url": "",
        }
        with patch.object(ui.st, "session_state", state):
            ui._reset_connection_scoped_results(config)

        self.assertNotIn("doctor_checks", state)
        self.assertNotIn("compose_status", state)
        self.assertNotIn("delete-dataset", state)
        self.assertNotIn("delete-dry-run", state)
        self.assertNotIn("delete-purge", state)
        self.assertNotIn("delete-confirmation", state)
        self.assertNotIn("publish_preview_signature", state)
        self.assertNotIn("store-project-path", state)
        self.assertNotIn("store-bucket", state)
        self.assertNotIn("store-region", state)
        self.assertNotIn("store-kms-key", state)
        self.assertNotIn("modify-dataset", state)
        self.assertNotIn("modify-metadata", state)
        self.assertNotIn("modify-images", state)
        self.assertNotIn("modify-masks", state)
        self.assertEqual(state["_connection_scope"], ui._connection_scope(config))

    def test_unknown_version_history_is_not_reported_as_an_empty_store(self):
        from dsimaging_admin import ui

        with patch.object(ui, "make_s3_client", return_value=object()), \
                patch.object(ui, "list_objects", return_value=[]), \
                patch.object(ui, "list_object_versions",
                             side_effect=PermissionError("denied")), \
                patch.object(ui.st, "error") as error, \
                patch.object(ui.st, "info") as info, \
                patch.object(ui.st, "header"):
            ui.render_delete({"bucket": "imaging-data"})

        self.assertIn("could not be listed", str(error.call_args).lower())
        self.assertFalse(any(
            "No datasets found" in str(call) for call in info.call_args_list))

    def test_sqs_checks_use_the_active_connection_credentials(self):
        import dsimaging_admin.ui as ui

        sqs = Mock()
        sqs.get_queue_attributes.return_value = {
            "Attributes": {"ApproximateNumberOfMessages": "0"},
        }
        config = {
            "sqs_queue_url": "https://queue.example",
            "access_key": "session-access",
            "secret_key": "session-secret",
            "region": "eu-west-1",
        }
        with patch.object(ui.st, "session_state", {}), \
                patch.object(ui, "create_sqs_client", return_value=sqs) as create:
            result = ui.sqs_depth(config)

        self.assertEqual(result["ApproximateNumberOfMessages"], "0")
        create.assert_called_once_with(
            "session-access", "session-secret", "eu-west-1")

    def test_dataset_listing_failure_is_not_an_empty_inventory(self):
        import dsimaging_admin.ui as ui

        with patch.object(ui, "dataset_rows", side_effect=RuntimeError("private")), \
                patch.object(ui.st, "error") as error, \
                patch.object(ui.st, "info") as info:
            rows = ui.safe_dataset_rows(object(), "imaging-data")

        self.assertIsNone(rows)
        self.assertNotIn("private", str(error.call_args))
        self.assertIn("Doctor", str(info.call_args))

    def test_aws_init_uses_complete_shared_provisioner(self):
        import dsimaging_admin.ui as ui

        report = {"ok": True, "steps": []}
        config = {
            "endpoint": "", "bucket": "imaging-data", "region": "eu-west-1",
            "access_key": "access", "secret_key": "secret",
        }
        with patch.object(ui, "provision_aws_store", return_value=report) as provision:
            observed = ui.init_aws_store(config, "kms-arn")

        self.assertIs(observed, report)
        provision.assert_called_once_with(
            None, "imaging-data", region="eu-west-1",
            access_key="access", secret_key="secret", kms_key="kms-arn",
        )

    def test_aws_init_rejects_non_aws_and_insecure_endpoints(self):
        import dsimaging_admin.ui as ui

        for endpoint in (
            "https://s3.amazonaws.com.attacker.invalid",
            "http://s3.eu-west-1.amazonaws.com",
        ):
            with self.subTest(endpoint=endpoint), self.assertRaisesRegex(
                    ValueError, "native HTTPS S3 endpoint"):
                ui.init_aws_store({
                    "endpoint": endpoint,
                    "bucket": "imaging-data",
                    "region": "eu-west-1",
                }, "")

    def test_modify_defers_existing_metadata_read_to_locked_rescan(self):
        from dsimaging_admin import ui

        transaction = {
            "staging_prefix": "datasets/study/.staging-owner",
            "backup_prefix": "datasets/study/.backup-owner",
            "publish_lock": {"key": "datasets/study/.publish-lock"},
        }
        with patch.object(ui, "make_s3_client", return_value=object()), \
                patch.object(ui, "_read_manifest_strict", return_value={}), \
                patch.object(ui, "metadata_contract_from_manifest"), \
                patch.object(ui, "list_objects", return_value=[]), \
                patch.object(ui, "scan_s3_images", return_value=[]), \
                patch.object(ui, "_atomic_upload_sources",
                             return_value=transaction), \
                patch.object(ui, "_rescan_dataset_artifacts",
                             return_value={"samples": 1, "masks": 0}) as rescan, \
                patch.object(ui, "_finish_atomic_publish", return_value=True):
            ui.modify_dataset(
                {"bucket": "imaging-data"}, "study", "", "", "", lambda _: None)

        self.assertIsNone(rescan.call_args.kwargs["extra_metadata"])

    def test_invalid_profile_file_fails_closed(self):
        import dsimaging_admin.ui as ui

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "config.yaml")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write("profiles: [broken\n")
            with self.assertRaisesRegex(ValueError, "Could not read"):
                ui.load_profiles(path)

    def test_empty_profiles_file_fails_closed(self):
        import dsimaging_admin.ui as ui

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "config.yaml")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write("profiles: {}\n")
            with self.assertRaisesRegex(ValueError, "defines no profiles"):
                ui.load_profiles(path)

    def test_invalid_aws_profile_section_fails_closed(self):
        import dsimaging_admin.ui as ui

        fixtures = (
            "profiles:\n  default:\n    aws: invalid\n",
            "default:\n  aws: invalid\n",
            "aws: invalid\n",
        )
        for content in fixtures:
            with self.subTest(content=content), tempfile.TemporaryDirectory() as tmpdir:
                path = os.path.join(tmpdir, "config.yaml")
                with open(path, "w", encoding="utf-8") as handle:
                    handle.write(content)
                with self.assertRaisesRegex(ValueError, "AWS profile section"):
                    ui.load_profiles(path)


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
                    },
                    "server_2": {
                        "endpoint": "https://store.example.org",
                        "bucket": "imaging-secondary",
                        "backend": "s3-compatible",
                        "controller_url": "https://controller.example.org",
                    },
                    "remote_minio": {
                        "endpoint": "https://minio.example.org",
                        "bucket": "imaging-remote",
                        "backend": "minio",
                    },
                    "aws_a": {
                        "bucket": "imaging-aws-a",
                        "region": "eu-west-1",
                        "backend": "aws",
                    },
                    "aws_b": {
                        "bucket": "imaging-aws-b",
                        "region": "eu-central-1",
                        "backend": "aws",
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

    def test_remote_minio_profile_disables_local_compose_controls(self):
        at = self.app()
        [item for item in at.selectbox if item.label == "Profile"][0] \
            .set_value("remote_minio").run()
        at.radio[0].set_value("Store administration").run()

        for label in ("Up", "Down", "Refresh status"):
            button = [item for item in at.button if item.label == label][0]
            self.assertTrue(button.disabled)

    def test_unowned_loopback_profile_disables_local_compose_controls(self):
        at = self.app()
        at.radio[0].set_value("Store administration").run()

        for label in ("Up", "Down", "Refresh status"):
            button = [item for item in at.button if item.label == label][0]
            self.assertTrue(button.disabled)

    def test_aws_kms_key_clears_when_profile_changes(self):
        at = self.app()
        profile = [item for item in at.selectbox if item.label == "Profile"][0]
        profile.set_value("aws_a").run()
        at.radio[0].set_value("Store administration").run()
        at.text_input(key="store-kms-key").set_value("arn:aws:kms:key/a").run()

        [item for item in at.selectbox if item.label == "Profile"][0] \
            .set_value("aws_b").run()

        self.assertEqual(at.text_input(key="store-kms-key").value, "")
        self.assertEqual(at.text_input(key="store-bucket").value, "imaging-aws-b")
        self.assertEqual(at.text_input(key="store-region").value, "eu-central-1")

    def test_aws_kms_key_clears_when_connection_region_changes(self):
        at = self.app()
        [item for item in at.selectbox if item.label == "Profile"][0] \
            .set_value("aws_a").run()
        at.radio[0].set_value("Store administration").run()
        at.text_input(key="store-kms-key").set_value("arn:aws:kms:key/a").run()

        sidebar_region = [
            item for item in at.text_input
            if item.label == "Region" and item.key != "store-region"
        ][0]
        sidebar_region.set_value("us-west-2").run()

        self.assertEqual(at.text_input(key="store-kms-key").value, "")
        self.assertEqual(at.text_input(key="store-region").value, "us-west-2")

    def test_publish_preview_scan_counts_fixture(self):
        at = self.app()
        at.radio[0].set_value("Publish").run()
        at.text_input(key="publish-dataset").set_value("preview_ct_v1").run()
        at.text_input(key="publish-source").set_value(str(FIXTURE)).run()
        [button for button in at.button if button.label == "Preview scan"][0].click().run()
        text = rendered_text(at)
        self.assertIn("case001", text)
        self.assertIn("Images", text)

    def test_changing_publish_inputs_invalidates_preflight(self):
        at = self.app()
        at.radio[0].set_value("Publish").run()
        at.text_input(key="publish-dataset").set_value("preview_ct_v1").run()
        at.text_input(key="publish-source").set_value(str(FIXTURE)).run()
        [button for button in at.button if button.label == "Preview scan"][0].click().run()
        publish = [button for button in at.button if button.label == "Publish"][0]
        self.assertFalse(publish.disabled)

        at.text_input(key="publish-dataset").set_value("changed_ct_v1").run()
        publish = [button for button in at.button if button.label == "Publish"][0]
        self.assertTrue(publish.disabled)

    def test_connection_secrets_clear_when_endpoint_or_profile_changes(self):
        at = self.app()
        [field for field in at.text_input if field.label == "Access key"][0] \
            .set_value("temporary-access").run()
        [field for field in at.text_input if field.label == "Secret key"][0] \
            .set_value("temporary-secret").run()

        [field for field in at.text_input if field.label == "Endpoint"][0] \
            .set_value("https://changed.example.org").run()
        self.assertEqual(
            [field for field in at.text_input if field.label == "Access key"][0].value,
            "",
        )
        self.assertEqual(
            [field for field in at.text_input if field.label == "Secret key"][0].value,
            "",
        )

        [field for field in at.text_input if field.label == "Access key"][0] \
            .set_value("second-temporary-access").run()
        [item for item in at.selectbox if item.label == "Profile"][0] \
            .set_value("server_2").run()
        self.assertEqual(
            [field for field in at.text_input if field.label == "Access key"][0].value,
            "",
        )

    def test_controller_token_clears_when_controller_url_changes(self):
        at = self.app()
        [field for field in at.text_input
         if field.label == "Controller operator token"][0] \
            .set_value("temporary-token").run()
        [field for field in at.text_input if field.label == "Controller URL"][0] \
            .set_value("https://changed-controller.example.org").run()
        self.assertEqual(
            [field for field in at.text_input
             if field.label == "Controller operator token"][0].value,
            "",
        )

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

    def test_profile_change_invalidates_destructive_delete_confirmation(self):
        at = self.app()
        at.radio[0].set_value("Delete").run()
        [item for item in at.checkbox if item.label == "Dry run"][0] \
            .set_value(False).run()
        [item for item in at.checkbox
         if item.label == "Purge all object versions and delete markers"][0] \
            .set_value(True).run()
        [field for field in at.text_input
         if field.label == "Type the dataset ID to confirm"][0] \
            .set_value("lung_ct_v1").run()
        self.assertFalse(
            [button for button in at.button
             if button.label == "Delete dataset"][0].disabled)

        [item for item in at.selectbox if item.label == "Profile"][0] \
            .set_value("server_2").run()

        self.assertTrue(
            [item for item in at.checkbox if item.label == "Dry run"][0].value)
        self.assertFalse(
            [item for item in at.checkbox
             if item.label == "Purge all object versions and delete markers"][0].value)
        self.assertEqual(
            [field for field in at.text_input
             if field.label == "Type the dataset ID to confirm"][0].value,
            "",
        )
        self.assertTrue(
            [button for button in at.button
             if button.label == "Delete dataset"][0].disabled)

    def test_profile_change_clears_modify_paths(self):
        at = self.app()
        at.radio[0].set_value("Modify / Rescan").run()
        at.text_input(key="modify-metadata").set_value("/tmp/metadata-a.csv").run()
        at.text_input(key="modify-images").set_value("/tmp/images-a").run()
        self.assertFalse(
            [button for button in at.button
             if button.label == "Apply modify"][0].disabled)

        [item for item in at.selectbox if item.label == "Profile"][0] \
            .set_value("server_2").run()

        self.assertEqual(at.text_input(key="modify-metadata").value, "")
        self.assertEqual(at.text_input(key="modify-images").value, "")
        self.assertTrue(
            [button for button in at.button
             if button.label == "Apply modify"][0].disabled)

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

    @unittest.skipUnless(HAS_MOTO, "moto is not installed")
    def test_ui_can_purge_history_after_current_dataset_was_deleted(self):
        from dsimaging_admin.cli import _delete_dataset_current

        with mock_aws():
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
            dataset_id = "history_only_ct_v1"
            key = f"datasets/{dataset_id}/manifest.yaml"
            s3.put_object(Bucket="imaging-data", Key=key, Body=b"first")
            s3.put_object(Bucket="imaging-data", Key=key, Body=b"second")
            _delete_dataset_current(s3, "imaging-data", dataset_id)

            at = AppTest.from_file(str(UI_PATH))
            at.session_state["_s3_client"] = s3
            at.run()
            at.radio[0].set_value("Delete").run()
            candidates = [
                item for item in at.selectbox
                if item.label == "Dataset to delete"
            ][0]
            self.assertIn(dataset_id, candidates.options)
            candidates.set_value(dataset_id).run()
            [item for item in at.checkbox if item.label == "Dry run"][0] \
                .set_value(False).run()
            [item for item in at.checkbox
             if item.label == "Purge all object versions and delete markers"][0] \
                .set_value(True).run()
            [field for field in at.text_input
             if field.label == "Type the dataset ID to confirm"][0] \
                .set_value(dataset_id).run()
            [button for button in at.button
             if button.label == "Delete dataset"][0].click().run()

            history = s3.list_object_versions(
                Bucket="imaging-data", Prefix=f"datasets/{dataset_id}/")
            self.assertEqual(
                history.get("Versions", []) + history.get("DeleteMarkers", []),
                [],
            )

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
