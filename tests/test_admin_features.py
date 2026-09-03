import datetime as dt
import hashlib
import io
import json
import os
import stat
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from click.testing import CliRunner

from dsimaging_admin.cli import main
from dsimaging_admin.manifest import (
    build_hash_index,
    build_sample_manifests,
    build_samples_metadata,
    generate_manifest,
)
from dsimaging_admin.s3 import (
    delete_keys,
    delete_object_versions,
    list_object_versions,
    list_objects,
)
from dsimaging_admin.store import (
    DEFAULT_CONTROLLER_IMAGE,
    DEFAULT_MC_IMAGE,
    DEFAULT_MINIO_IMAGE,
    INIT_BUCKET_SCRIPT,
    init_store,
    load_store_config,
)
from dsimaging_admin.verify import verify_dataset


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
    def __init__(self, objects):
        self.objects = objects

    def get_object(self, Bucket, Key):
        return {"Body": FakeBody(self.objects[Key]["body"])}

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise KeyError(Key)
        obj = self.objects[Key]
        return {
            "ContentLength": len(obj["body"]),
            "LastModified": obj["last_modified"],
            "ETag": f'"{obj.get("etag", "etag")}"',
            "VersionId": obj.get("version_id"),
        }

    def get_paginator(self, name):
        if name != "list_objects_v2":
            raise ValueError(name)
        return FakePaginator(self.objects)


def parquet_bytes(table):
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    return sink.getvalue().to_pybytes()


def publication_artifacts(bucket, prefix, samples, now):
    metadata_input = pa.table({
        "sample_id": [sample["sample_id"] for sample in samples],
        "patient_id": [
            f"patient-{index}" for index, _ in enumerate(samples, start=1)
        ],
    })
    metadata = build_samples_metadata(
        samples, metadata_input, privacy_unit_col="patient_id")
    manifest = generate_manifest(
        prefix.rsplit("/", 1)[-1], bucket, prefix,
        privacy_unit_col="patient_id",
    )
    return {
        f"{prefix}/manifest.yaml": {
            "body": yaml.safe_dump(manifest, sort_keys=False).encode("utf-8"),
            "last_modified": now,
        },
        f"{prefix}/metadata/samples.parquet": {
            "body": parquet_bytes(metadata), "last_modified": now,
        },
        f"{prefix}/metadata/sample_manifests.parquet": {
            "body": parquet_bytes(build_sample_manifests(samples)),
            "last_modified": now,
        },
    }


class AdminFeatureTests(unittest.TestCase):
    def test_deletion_reports_backend_failures(self):
        class FailingS3:
            def delete_objects(self, **kwargs):
                return {"Errors": [{"Code": "AccessDenied"}]}

        s3 = FailingS3()
        with self.assertRaisesRegex(RuntimeError, "object deletion failed"):
            delete_keys(s3, "imaging-data", ["datasets/study/manifest.yaml"])
        with self.assertRaisesRegex(RuntimeError, "version deletion failed"):
            delete_object_versions(s3, "imaging-data", [{
                "key": "datasets/study/manifest.yaml",
                "version_id": "v1",
            }])

    def test_version_listing_propagates_backend_failures(self):
        class FailingPaginator:
            def paginate(self, **kwargs):
                raise PermissionError("version listing denied")

        class FailingS3:
            def get_paginator(self, name):
                self.name = name
                return FailingPaginator()

        s3 = FailingS3()
        with self.assertRaisesRegex(PermissionError, "listing denied"):
            list_object_versions(s3, "imaging-data", "datasets/study/")
        self.assertEqual(s3.name, "list_object_versions")

    def test_versioned_listing_uses_one_coherent_head_snapshot(self):
        listed_at = dt.datetime(2026, 5, 12, tzinfo=dt.timezone.utc)
        headed_at = dt.datetime(2026, 5, 13, tzinfo=dt.timezone.utc)

        class StalePaginator:
            def paginate(self, Bucket, Prefix):
                return [{"Contents": [{
                    "Key": f"{Prefix}case001.nii.gz",
                    "Size": 3,
                    "LastModified": listed_at,
                    "ETag": '"listed-etag"',
                }]}]

        class HeadedS3:
            def get_paginator(self, name):
                if name != "list_objects_v2":
                    raise ValueError(name)
                return StalePaginator()

            def head_object(self, Bucket, Key):
                return {
                    "ContentLength": 7,
                    "LastModified": headed_at,
                    "ETag": '"headed-etag"',
                    "VersionId": "version-2",
                }

        objects = list_objects(
            HeadedS3(), "imaging-data", "datasets/study/source/images/",
            include_version_ids=True,
        )

        self.assertEqual(objects, [{
            "key": "datasets/study/source/images/case001.nii.gz",
            "size": 7,
            "last_modified": headed_at.isoformat(),
            "etag": "headed-etag",
            "version_id": "version-2",
            "content_type": None,
        }])

    def test_verify_dataset_rejects_noncanonical_dataset_id(self):
        with self.assertRaisesRegex(ValueError, "dataset_id must match"):
            verify_dataset(FakeS3({}), "imaging-data", "../other")

    def test_store_init_generates_compose_project(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = init_store(
                tmpdir,
                controller_image="example/dsimaging-store-controller:test",
                bucket="custom-bucket",
            )
            loaded = load_store_config(tmpdir)
            with open(os.path.join(tmpdir, "docker-compose.yml"), encoding="utf-8") as f:
                compose = f.read()
            with open(os.path.join(tmpdir, ".env"), encoding="utf-8") as f:
                env = f.read()
            env_mode = stat.S_IMODE(
                os.stat(os.path.join(tmpdir, ".env")).st_mode)
            with open(os.path.join(tmpdir, "init-bucket.sh"), encoding="utf-8") as f:
                init_script = f.read()

        self.assertEqual(cfg.bucket, "custom-bucket")
        self.assertEqual(loaded.bucket, "custom-bucket")
        self.assertTrue(cfg.access_key.startswith("dsimg"))
        self.assertNotEqual(cfg.access_key, "minioadmin")
        self.assertNotEqual(cfg.secret_key, "minioadmin123")
        self.assertTrue(cfg.controller_token)
        self.assertTrue(cfg.webhook_token)
        self.assertEqual(cfg.controller_token, loaded.controller_token)
        self.assertEqual(cfg.webhook_token, loaded.webhook_token)
        self.assertNotIn("controller_token", cfg.to_dict())
        self.assertNotIn("webhook_token", cfg.to_dict())
        self.assertEqual(env_mode, 0o600)
        self.assertIn("example/dsimaging-store-controller:test", env)
        self.assertIn("DSIMAGING_CONTROLLER_TOKEN=", env)
        self.assertIn("DSIMAGING_CONTROLLER_TOKEN", compose)
        self.assertIn("DSIMAGING_WEBHOOK_TOKEN=", env)
        self.assertIn(
            'MINIO_NOTIFY_WEBHOOK_AUTH_TOKEN_DSIMAGING: '
            '"Bearer ${DSIMAGING_WEBHOOK_TOKEN:',
            compose,
        )
        self.assertIn("DSIMAGING_WEBHOOK_TOKEN", compose)
        self.assertIn('127.0.0.1:${MINIO_PORT:-9000}:9000', compose)
        self.assertIn(
            '127.0.0.1:${MINIO_CONSOLE_PORT:-9001}:9001', compose)
        self.assertIn('127.0.0.1:${CONTROLLER_PORT:-8080}:8080', compose)
        self.assertIn("controller", compose)
        self.assertIn("minio", compose)
        self.assertIn("mc event remove", init_script)
        self.assertNotIn("grep -qi", init_script)

    def test_store_default_images_are_versioned_and_digest_pinned(self):
        for image in (
                DEFAULT_CONTROLLER_IMAGE, DEFAULT_MINIO_IMAGE,
                DEFAULT_MC_IMAGE):
            with self.subTest(image=image):
                self.assertNotIn(":latest", image)
                self.assertRegex(image, r":[^@]+@sha256:[0-9a-f]{64}$")

    def test_store_init_can_use_local_store_source(self):
        with tempfile.TemporaryDirectory() as tmpdir, tempfile.TemporaryDirectory() as source:
            os.makedirs(os.path.join(source, "controller"))
            init_store(tmpdir, store_source=source)
            with open(os.path.join(tmpdir, "docker-compose.yml"), encoding="utf-8") as f:
                compose = f.read()

        self.assertIn("build:", compose)
        self.assertIn("/controller", compose)

    def test_store_force_init_preserves_generated_credentials(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            first = init_store(tmpdir)
            second = init_store(tmpdir, force=True)

        self.assertEqual(second.access_key, first.access_key)
        self.assertEqual(second.secret_key, first.secret_key)
        self.assertEqual(second.controller_token, first.controller_token)
        self.assertEqual(second.webhook_token, first.webhook_token)

    def test_store_init_never_places_secret_in_mc_process_arguments(self):
        self.assertNotIn(
            'mc alias set local "${MINIO_ENDPOINT}"', INIT_BUCKET_SCRIPT)
        self.assertIn("| mc alias import local/", INIT_BUCKET_SCRIPT)

    def test_store_init_rejects_dotenv_injection_and_invalid_ports(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaisesRegex(ValueError, "unsafe for a Compose"):
                init_store(tmpdir, access_key="valid\nINJECTED=value")
            with self.assertRaisesRegex(ValueError, "ports must be distinct"):
                init_store(tmpdir, minio_port=9000, console_port=9000)

    def test_store_source_path_is_quoted_in_generated_yaml(self):
        with tempfile.TemporaryDirectory() as tmpdir, \
                tempfile.TemporaryDirectory(prefix="store source: ") as source:
            os.makedirs(os.path.join(source, "controller"))
            init_store(tmpdir, store_source=source)
            with open(os.path.join(tmpdir, "docker-compose.yml"),
                      encoding="utf-8") as handle:
                compose = yaml.safe_load(handle)

        self.assertEqual(
            compose["services"]["controller"]["build"]["context"],
            os.path.join(os.path.realpath(source), "controller"),
        )

    def test_cli_store_init_requires_explicit_path(self):
        runner = CliRunner()
        result = runner.invoke(main, ["store", "init"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Missing argument", result.output)

    def test_cli_store_doctor_json_preserves_failure_exit_code(self):
        payload = {"ok": False, "docker": {}, "controller": {}, "s3": {}}
        with patch("dsimaging_admin.cli.create_client", return_value=object()), \
                patch("dsimaging_admin.cli.store_doctor", return_value=payload):
            result = CliRunner().invoke(
                main, ["store", "doctor", ".", "--output", "json"])

        self.assertEqual(result.exit_code, 1, result.output)
        self.assertEqual(json.loads(result.output), payload)

    def test_cli_doctor_json_preserves_failure_exit_code(self):
        payload = {
            "ok": False,
            "checks": [{
                "name": "S3 connectivity", "status": "FAIL", "detail": "down",
            }],
        }
        with patch("dsimaging_admin.cli.create_client", return_value=object()), \
                patch("dsimaging_admin.cli._doctor_result", return_value=payload):
            result = CliRunner().invoke(main, ["doctor", "--output", "json"])

        self.assertEqual(result.exit_code, 1, result.output)
        self.assertEqual(json.loads(result.output), payload)

    def test_cli_dataset_status_json_fails_for_controller_error(self):
        controllers = [
            {"dataset_id": "study", "has_error": True},
            {"error": "controller unavailable"},
        ]
        for controller in controllers:
            with self.subTest(controller=controller), \
                    patch("dsimaging_admin.cli.create_client", return_value=object()), \
                    patch("dsimaging_admin.cli._dataset_status", return_value={
                        "dataset_id": "study", "controller": controller,
                    }):
                result = CliRunner().invoke(
                    main, ["dataset", "status", "study", "--output", "json"])

            self.assertEqual(result.exit_code, 1, result.output)
            self.assertEqual(
                json.loads(result.output)["controller"], controller)

    def test_cli_store_init_derives_minio_port_from_endpoint(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runner = CliRunner()
            result = runner.invoke(main, [
                "--endpoint", "http://127.0.0.1:9100",
                "--skip-controller",
                "store", "init", tmpdir,
            ])
            self.assertEqual(result.exit_code, 0, result.output)
            with open(os.path.join(tmpdir, ".env"), encoding="utf-8") as f:
                env = f.read()
        self.assertIn("MINIO_PORT=9100", env)

    def test_cli_store_init_explicit_minio_port_wins(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runner = CliRunner()
            result = runner.invoke(main, [
                "--endpoint", "http://127.0.0.1:9100",
                "--skip-controller",
                "store", "init", tmpdir,
                "--minio-port", "9200",
            ])
            self.assertEqual(result.exit_code, 0, result.output)
            with open(os.path.join(tmpdir, ".env"), encoding="utf-8") as f:
                env = f.read()
        self.assertIn("MINIO_PORT=9200", env)

    def test_cli_store_init_does_not_reuse_connection_credentials(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runner = CliRunner()
            result = runner.invoke(
                main,
                ["--skip-controller", "store", "init", tmpdir],
                env={
                    "DSIMAGING_ACCESS_KEY": "minioadmin",
                    "DSIMAGING_SECRET_KEY": "minioadmin123",
                },
            )
            self.assertEqual(result.exit_code, 0, result.output)
            generated = load_store_config(tmpdir)

        self.assertTrue(generated.access_key.startswith("dsimg"))
        self.assertNotEqual(generated.access_key, "minioadmin")
        self.assertNotEqual(generated.secret_key, "minioadmin123")

    def test_cli_store_init_accepts_explicit_new_store_credentials(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runner = CliRunner()
            result = runner.invoke(main, [
                "--skip-controller", "store", "init", tmpdir,
                "--access-key", "explicit-user",
                "--secret-key", "explicit-secret-value",
            ])
            self.assertEqual(result.exit_code, 0, result.output)
            generated = load_store_config(tmpdir)

        self.assertEqual(generated.access_key, "explicit-user")
        self.assertEqual(generated.secret_key, "explicit-secret-value")

    def test_cli_store_up_rejects_non_store_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runner = CliRunner()
            result = runner.invoke(main, ["store", "up", tmpdir])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("is not a dsimaging-store project", result.output)
        self.assertIn("docker-compose.yml/.env", result.output)

    def test_verify_dataset_accepts_matching_single_file(self):
        bucket = "imaging-data"
        prefix = "datasets/study_ct_v1"
        payload = b"nifti"
        content_hash = hashlib.sha256(payload).hexdigest()
        sample = {
            "sample_id": "case001",
            "source_kind": "single_file",
            "primary_filename": "case001.nii.gz",
            "uri_path": "case001.nii.gz",
            "files": [{"path": "case001.nii.gz", "role": "primary"}],
            "content_hash": content_hash,
            "size": len(payload),
            "etag": "same-etag",
        }
        index = build_hash_index([sample], bucket, prefix)
        now = dt.datetime(2026, 5, 13, tzinfo=dt.timezone.utc)
        objects = {
            f"{prefix}/source/images/case001.nii.gz": {
                "body": payload,
                "etag": "same-etag",
                "last_modified": now,
            },
            f"{prefix}/indexes/content_hash_index.parquet": {
                "body": parquet_bytes(index),
                "last_modified": now,
            },
        }
        objects.update(publication_artifacts(bucket, prefix, [sample], now))
        s3 = FakeS3(objects)

        result = verify_dataset(s3, bucket, "study_ct_v1")

        self.assertTrue(result.ok)
        self.assertEqual(result.checked, 1)
        self.assertEqual(result.mismatched, 0)

    def test_verify_dataset_reports_mismatch(self):
        bucket = "imaging-data"
        prefix = "datasets/study_ct_v1"
        sample = {
            "sample_id": "case001",
            "source_kind": "single_file",
            "primary_filename": "case001.nii.gz",
            "uri_path": "case001.nii.gz",
            "files": [{"path": "case001.nii.gz", "role": "primary"}],
            "content_hash": hashlib.sha256(b"expected").hexdigest(),
            "size": len(b"actual"),
        }
        index = build_hash_index([sample], bucket, prefix)
        now = dt.datetime(2026, 5, 13, tzinfo=dt.timezone.utc)
        objects = {
            f"{prefix}/source/images/case001.nii.gz": {
                "body": b"actual",
                "last_modified": now,
            },
            f"{prefix}/indexes/content_hash_index.parquet": {
                "body": parquet_bytes(index),
                "last_modified": now,
            },
        }
        objects.update(publication_artifacts(bucket, prefix, [sample], now))
        s3 = FakeS3(objects)

        result = verify_dataset(s3, bucket, "study_ct_v1")

        self.assertFalse(result.ok)
        self.assertEqual(result.mismatched, 1)

    def test_verify_quick_does_not_trust_etag_or_null_version(self):
        bucket = "imaging-data"
        prefix = "datasets/study_ct_v1"
        sample = {
            "sample_id": "case001",
            "source_kind": "single_file",
            "primary_filename": "case001.nii.gz",
            "uri_path": "case001.nii.gz",
            "files": [{"path": "case001.nii.gz", "role": "primary"}],
            "content_hash": hashlib.sha256(b"expected").hexdigest(),
            "size": len(b"actual"),
            "version_id": "null",
            "etag": "same-etag",
        }
        index = build_hash_index([sample], bucket, prefix)
        now = dt.datetime(2026, 5, 13, tzinfo=dt.timezone.utc)
        objects = {
            f"{prefix}/source/images/case001.nii.gz": {
                "body": b"actual",
                "etag": "same-etag",
                "version_id": "null",
                "last_modified": now,
            },
            f"{prefix}/indexes/content_hash_index.parquet": {
                "body": parquet_bytes(index),
                "last_modified": now,
            },
        }
        objects.update(publication_artifacts(bucket, prefix, [sample], now))
        s3 = FakeS3(objects)

        result = verify_dataset(s3, bucket, "study_ct_v1", quick=True)

        self.assertFalse(result.ok)
        self.assertEqual(result.mismatched, 1)
        self.assertEqual(result.quick_ok, 0)

    def test_verify_quick_rechecks_legacy_unattested_version_hash(self):
        bucket = "imaging-data"
        prefix = "datasets/study_ct_v1"
        payload = b"actual"
        sample = {
            "sample_id": "case001",
            "source_kind": "single_file",
            "primary_filename": "case001.nii.gz",
            "uri_path": "case001.nii.gz",
            "files": [{"path": "case001.nii.gz", "role": "primary"}],
            "content_hash": hashlib.sha256(b"recorded-at-upload").hexdigest(),
            "size": len(payload),
            "version_id": "immutable-version-1",
            "etag": "same-etag",
        }
        now = dt.datetime(2026, 5, 13, tzinfo=dt.timezone.utc)
        objects = {
            f"{prefix}/source/images/case001.nii.gz": {
                "body": payload,
                "etag": "same-etag",
                "version_id": "immutable-version-1",
                "last_modified": now,
            },
            f"{prefix}/indexes/content_hash_index.parquet": {
                "body": parquet_bytes(build_hash_index([sample], bucket, prefix)),
                "last_modified": now,
            },
        }
        objects.update(publication_artifacts(bucket, prefix, [sample], now))

        result = verify_dataset(
            FakeS3(objects), bucket, "study_ct_v1", quick=True)

        self.assertFalse(result.ok)
        self.assertEqual(result.mismatched, 1)
        self.assertEqual(result.quick_ok, 0)

    def test_verify_quick_accepts_version_bound_hash(self):
        bucket = "imaging-data"
        prefix = "datasets/study_ct_v1"
        payload = b"actual"
        sample = {
            "sample_id": "case001",
            "source_kind": "single_file",
            "primary_filename": "case001.nii.gz",
            "uri_path": "case001.nii.gz",
            "files": [{"path": "case001.nii.gz", "role": "primary"}],
            "content_hash": hashlib.sha256(payload).hexdigest(),
            "size": len(payload),
            "version_id": "immutable-version-1",
            "content_hash_version_id": "immutable-version-1",
            "etag": "same-etag",
        }
        now = dt.datetime(2026, 5, 13, tzinfo=dt.timezone.utc)
        objects = {
            f"{prefix}/source/images/case001.nii.gz": {
                "body": payload,
                "etag": "same-etag",
                "version_id": "immutable-version-1",
                "last_modified": now,
            },
            f"{prefix}/indexes/content_hash_index.parquet": {
                "body": parquet_bytes(build_hash_index([sample], bucket, prefix)),
                "last_modified": now,
            },
        }
        objects.update(publication_artifacts(bucket, prefix, [sample], now))

        result = verify_dataset(
            FakeS3(objects), bucket, "study_ct_v1", quick=True)

        self.assertTrue(result.ok)
        self.assertEqual(result.quick_ok, 1)

    def test_verify_rejects_corrupt_publication_metadata(self):
        bucket = "imaging-data"
        prefix = "datasets/study_ct_v1"
        payload = b"image"
        sample = {
            "sample_id": "case001", "source_kind": "single_file",
            "primary_filename": "case001.nii.gz",
            "uri_path": "case001.nii.gz",
            "files": [{"path": "case001.nii.gz", "role": "primary"}],
            "content_hash": hashlib.sha256(payload).hexdigest(),
            "size": len(payload),
        }
        now = dt.datetime(2026, 5, 13, tzinfo=dt.timezone.utc)
        objects = publication_artifacts(bucket, prefix, [sample], now)
        objects.update({
            f"{prefix}/source/images/case001.nii.gz": {
                "body": payload, "last_modified": now,
            },
            f"{prefix}/indexes/content_hash_index.parquet": {
                "body": parquet_bytes(build_hash_index(
                    [sample], bucket, prefix)),
                "last_modified": now,
            },
        })
        objects[f"{prefix}/metadata/samples.parquet"]["body"] = b"corrupt"

        result = verify_dataset(FakeS3(objects), bucket, "study_ct_v1")

        self.assertFalse(result.ok)
        self.assertTrue(any(
            "publication contract" in issue.detail for issue in result.issues))

    def test_verify_rejects_cross_collection_and_manifest_index_drift(self):
        bucket = "imaging-data"
        prefix = "datasets/study_ct_v1"
        payload = b"image"
        sample = {
            "sample_id": "case001", "source_kind": "single_file",
            "primary_filename": "case001.nii.gz",
            "uri_path": "case001.nii.gz",
            "files": [{"path": "case001.nii.gz", "role": "primary"}],
            "content_hash": hashlib.sha256(payload).hexdigest(),
            "size": len(payload),
        }
        now = dt.datetime(2026, 5, 13, tzinfo=dt.timezone.utc)

        for corruption in (
                "manifest-root", "index-uri", "manifest-hash", "index-size",
                "publish-lock"):
            with self.subTest(corruption=corruption):
                objects = publication_artifacts(
                    bucket, prefix, [sample], now)
                objects[f"{prefix}/source/images/case001.nii.gz"] = {
                    "body": payload, "last_modified": now,
                }
                index = build_hash_index([sample], bucket, prefix)
                if corruption == "manifest-root":
                    manifest = yaml.safe_load(
                        objects[f"{prefix}/manifest.yaml"]["body"])
                    manifest["assets"]["images"]["uri"] = (
                        "s3://imaging-data/datasets/other/source/images/")
                    objects[f"{prefix}/manifest.yaml"]["body"] = (
                        yaml.safe_dump(manifest).encode("utf-8"))
                elif corruption == "index-uri":
                    index = index.set_column(
                        index.column_names.index("uri"), "uri",
                        pa.array([
                            "s3://imaging-data/datasets/other/source/images/"
                            "case001.nii.gz"
                        ]),
                    )
                elif corruption == "index-size":
                    index = index.set_column(
                        index.column_names.index("size"), "size",
                        pa.array([len(payload) + 1], type=pa.int64()),
                    )
                elif corruption == "publish-lock":
                    objects[f"{prefix}/.publish-lock"] = {
                        "body": b'{"owner":"active"}', "last_modified": now,
                    }
                else:
                    bad_sample = {**sample, "content_hash": "0" * 64}
                    objects[
                        f"{prefix}/metadata/sample_manifests.parquet"
                    ]["body"] = parquet_bytes(
                        build_sample_manifests([bad_sample]))
                objects[f"{prefix}/indexes/content_hash_index.parquet"] = {
                    "body": parquet_bytes(index), "last_modified": now,
                }

                result = verify_dataset(
                    FakeS3(objects), bucket, "study_ct_v1")

                self.assertFalse(result.ok)
                self.assertTrue(any(
                    issue.issue == "mismatch" for issue in result.issues))

    def test_verify_fraction_does_not_report_unselected_rows_as_extra(self):
        bucket = "imaging-data"
        prefix = "datasets/study_ct_v1"
        now = dt.datetime(2026, 5, 13, tzinfo=dt.timezone.utc)
        samples = []
        objects = {}
        for sample_id in ("case001", "case002"):
            payload = sample_id.encode("utf-8")
            samples.append({
                "sample_id": sample_id, "source_kind": "single_file",
                "primary_filename": f"{sample_id}.nii.gz",
                "uri_path": f"{sample_id}.nii.gz",
                "files": [{
                    "path": f"{sample_id}.nii.gz", "role": "primary",
                }],
                "content_hash": hashlib.sha256(payload).hexdigest(),
                "size": len(payload),
            })
            objects[f"{prefix}/source/images/{sample_id}.nii.gz"] = {
                "body": payload, "last_modified": now,
            }
        objects[f"{prefix}/indexes/content_hash_index.parquet"] = {
            "body": parquet_bytes(build_hash_index(samples, bucket, prefix)),
            "last_modified": now,
        }
        objects.update(publication_artifacts(bucket, prefix, samples, now))

        result = verify_dataset(
            FakeS3(objects), bucket, "study_ct_v1", sample_fraction=0.5)

        self.assertTrue(result.ok)
        self.assertEqual(result.checked, 1)
        self.assertEqual(result.skipped, 1)
        self.assertEqual(result.extra, 0)


if __name__ == "__main__":
    unittest.main()
