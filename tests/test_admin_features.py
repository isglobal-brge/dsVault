import datetime as dt
import hashlib
import io
import os
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq
from click.testing import CliRunner

from dsimaging_admin.cli import main
from dsimaging_admin.manifest import build_hash_index
from dsimaging_admin.store import init_store, load_store_config
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
        }

    def get_paginator(self, name):
        if name != "list_objects_v2":
            raise ValueError(name)
        return FakePaginator(self.objects)


def parquet_bytes(table):
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    return sink.getvalue().to_pybytes()


class AdminFeatureTests(unittest.TestCase):
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

        self.assertEqual(cfg.bucket, "custom-bucket")
        self.assertEqual(loaded.bucket, "custom-bucket")
        self.assertIn("example/dsimaging-store-controller:test", env)
        self.assertIn("controller", compose)
        self.assertIn("minio", compose)

    def test_store_init_can_use_local_store_source(self):
        with tempfile.TemporaryDirectory() as tmpdir, tempfile.TemporaryDirectory() as source:
            os.makedirs(os.path.join(source, "controller"))
            init_store(tmpdir, store_source=source)
            with open(os.path.join(tmpdir, "docker-compose.yml"), encoding="utf-8") as f:
                compose = f.read()

        self.assertIn("build:", compose)
        self.assertIn("/controller", compose)

    def test_cli_store_init_requires_explicit_path(self):
        runner = CliRunner()
        result = runner.invoke(main, ["store", "init"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Missing argument", result.output)

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
        s3 = FakeS3({
            f"{prefix}/source/images/case001.nii.gz": {
                "body": payload,
                "etag": "same-etag",
                "last_modified": now,
            },
            f"{prefix}/indexes/content_hash_index.parquet": {
                "body": parquet_bytes(index),
                "last_modified": now,
            },
        })

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
        s3 = FakeS3({
            f"{prefix}/source/images/case001.nii.gz": {
                "body": b"actual",
                "last_modified": now,
            },
            f"{prefix}/indexes/content_hash_index.parquet": {
                "body": parquet_bytes(index),
                "last_modified": now,
            },
        })

        result = verify_dataset(s3, bucket, "study_ct_v1")

        self.assertFalse(result.ok)
        self.assertEqual(result.mismatched, 1)

    def test_verify_quick_accepts_unchanged_etag(self):
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
            "etag": "same-etag",
        }
        index = build_hash_index([sample], bucket, prefix)
        now = dt.datetime(2026, 5, 13, tzinfo=dt.timezone.utc)
        s3 = FakeS3({
            f"{prefix}/source/images/case001.nii.gz": {
                "body": b"actual",
                "etag": "same-etag",
                "last_modified": now,
            },
            f"{prefix}/indexes/content_hash_index.parquet": {
                "body": parquet_bytes(index),
                "last_modified": now,
            },
        })

        result = verify_dataset(s3, bucket, "study_ct_v1", quick=True)

        self.assertTrue(result.ok)
        self.assertEqual(result.quick_ok, 1)


if __name__ == "__main__":
    unittest.main()
