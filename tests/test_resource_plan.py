import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml
from click.testing import CliRunner

from dsimaging_admin.cli import main
from dsimaging_admin.manifest import generate_manifest
from dsimaging_admin.resources import (
    build_armadillo_resource_plan,
    build_direct_resource_url,
)
from dsimaging_admin.store import init_store

try:
    import boto3
    from moto import mock_aws
    HAS_MOTO = True
except Exception:
    boto3 = None
    mock_aws = None
    HAS_MOTO = False


class ResourcePlanTests(unittest.TestCase):
    def test_resource_location_matches_dsimaging_consumer_contract(self):
        url = build_direct_resource_url(
            "study-v1", endpoint="https://s3.example.org:9443",
            bucket="site-images", region="eu-west-1",
        )
        self.assertEqual(
            url,
            "imaging+dataset://study-v1?"
            "endpoint=https%3A%2F%2Fs3.example.org%3A9443&"
            "bucket=site-images&prefix=datasets%2Fstudy-v1&region=eu-west-1",
        )

        invalid = (
            {"endpoint": "https://s3.example.org/minio"},
            {"bucket": "Bad_Bucket"},
            {"region": "eu_west"},
        )
        base = {
            "dataset_id": "study-v1",
            "endpoint": "https://s3.example.org",
            "bucket": "site-images",
            "region": "eu-west-1",
        }
        for override in invalid:
            with self.subTest(override=override), self.assertRaises(ValueError):
                build_direct_resource_url(**(base | override))

        with self.assertRaises(ValueError):
            build_armadillo_resource_plan(
                dataset_id="study..v1", profile="site", bucket="site-images",
                endpoint="https://s3.example.org", region="eu-west-1",
                project="imaging", resource_name=None,
                armadillo_url="https://armadillo.example.org",
                credentials_ref="imaging_store_ro",
                manifest_schema_version=1,
            )

    def test_help_exposes_read_only_resource_plan_options(self):
        result = CliRunner().invoke(
            main, ["dataset", "resource-plan", "--help"])

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("--target [opal|armadillo]", result.output)
        self.assertIn("--resource-endpoint", result.output)
        self.assertIn("--armadillo-url", result.output)
        self.assertIn("--credentials-ref", result.output)
        self.assertIn("does not register", result.output)

    def test_opal_plan_uses_selected_local_profile_without_printing_secrets(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = init_store(os.path.join(tmpdir, "store"))
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(yaml.safe_dump({
                "default_profile": "study-store",
                "profiles": {"study-store": {
                    "kind": "local-store", "store_path": store.path,
                }},
            }), encoding="utf-8")
            s3 = object()
            manifest = {"schema_version": 1}
            with patch.dict(os.environ, {"DSIMAGING_CONFIG": str(config_path)}), \
                    patch("dsimaging_admin.cli.create_client", return_value=s3) as create, \
                    patch("dsimaging_admin.cli._read_manifest_strict",
                          return_value=manifest) as read_manifest:
                result = CliRunner().invoke(main, [
                    "--profile", "study-store",
                    "dataset", "resource-plan", "study_ct_v1",
                    "--target", "opal", "--project", "IMAGING",
                    "--name", "study_images",
                    "--resource-endpoint", "http://minio:9000",
                ])

        self.assertEqual(result.exit_code, 0, result.output)
        create.assert_called_once_with(
            store.endpoint, store.access_key, store.secret_key, "")
        read_manifest.assert_called_once_with(
            s3, store.bucket, "datasets/study_ct_v1")
        plan = yaml.safe_load(result.output)
        self.assertEqual(plan, {
            "target": "opal",
            "registered": False,
            "profile": "study-store",
            "dataset_id": "study_ct_v1",
            "manifest": {
                "uri": "s3://imaging-data/datasets/study_ct_v1/manifest.yaml",
                "schema_version": 1,
                "validated": True,
            },
            "resource": {
                "project": "IMAGING",
                "name": "study_images",
                "url": (
                    "imaging+dataset://study_ct_v1?"
                    "endpoint=http%3A%2F%2Fminio%3A9000&"
                    "bucket=imaging-data&prefix=datasets%2Fstudy_ct_v1"
                ),
            },
            "credential_environment": {
                "identity_env": "DSIMAGING_RESOURCE_ACCESS_KEY",
                "secret_env": "DSIMAGING_RESOURCE_SECRET_KEY",
            },
            "instructions": [
                "Install dsImaging on the Opal R server so its Resource form and resolver are available.",
                "Set the two environment variables to dedicated read-only object-store credentials.",
                "Run the R command with an authenticated opalr connection named opal.",
            ],
            "commands": [(
                'opalr::opal.resource_create(opal, project = "IMAGING", '
                'name = "study_images", url = "imaging+dataset://study_ct_v1?'
                'endpoint=http%3A%2F%2Fminio%3A9000&bucket=imaging-data&'
                'prefix=datasets%2Fstudy_ct_v1", identity = '
                'Sys.getenv("DSIMAGING_RESOURCE_ACCESS_KEY"), secret = '
                'Sys.getenv("DSIMAGING_RESOURCE_SECRET_KEY"))'
            )],
        })
        for secret in (
            store.access_key, store.secret_key, store.controller_token,
            store.webhook_token,
        ):
            self.assertNotIn(secret, result.output)

    def test_loopback_profile_requires_explicit_consumer_endpoint(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = init_store(os.path.join(tmpdir, "store"))
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(yaml.safe_dump({
                "default_profile": "local",
                "profiles": {"local": {
                    "kind": "local-store", "store_path": store.path,
                }},
            }), encoding="utf-8")
            with patch.dict(os.environ, {"DSIMAGING_CONFIG": str(config_path)}), \
                    patch("dsimaging_admin.cli.create_client") as create:
                result = CliRunner().invoke(main, [
                    "dataset", "resource-plan", "study",
                    "--target", "opal", "--project", "IMAGING",
                ])

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("--resource-endpoint", result.output)
        self.assertIn("DataSHIELD server", result.output)
        create.assert_not_called()

    def test_manifest_failure_produces_no_plan(self):
        s3 = object()
        with patch("dsimaging_admin.cli.create_client", return_value=s3), \
                patch("dsimaging_admin.cli._read_manifest_strict",
                      side_effect=ValueError("dataset manifest is corrupt")):
            result = CliRunner().invoke(main, [
                "--backend", "s3-compatible",
                "--endpoint", "https://s3.example.org",
                "dataset", "resource-plan", "study",
                "--target", "opal", "--project", "IMAGING",
            ])

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("dataset manifest is corrupt", result.output)
        self.assertNotIn("registered:", result.output)

    def test_armadillo_plan_matches_marker_and_registry_contracts(self):
        s3 = object()
        manifest = {"schema_version": 1}
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(yaml.safe_dump({
                "default_profile": "site-b",
                "profiles": {"site-b": {
                    "backend": "s3-compatible",
                    "endpoint": "https://admin-s3.example.org",
                    "bucket": "site-images",
                    "region": "eu-west-1",
                    "access_key": "legacy-admin-access",
                    "secret_key": "legacy-admin-secret",
                }},
            }), encoding="utf-8")
            with patch.dict(os.environ, {"DSIMAGING_CONFIG": str(config_path)}), \
                    patch("dsimaging_admin.cli.create_client", return_value=s3), \
                    patch("dsimaging_admin.cli._read_manifest_strict",
                          return_value=manifest):
                result = CliRunner().invoke(main, [
                    "--profile", "site-b",
                    "dataset", "resource-plan", "imaging.contract-v1",
                    "--target", "armadillo", "--project", "imaging",
                    "--armadillo-url", "https://armadillo.example.org/",
                    "--resource-endpoint", "http://minio:9000",
                    "--credentials-ref", "imaging_store_ro",
                ])

        self.assertEqual(result.exit_code, 0, result.output)
        plan = yaml.safe_load(result.output)
        self.assertEqual(plan["target"], "armadillo")
        self.assertFalse(plan["registered"])
        self.assertEqual(plan["marker"], {
            "project": "imaging",
            "folder": "markers",
            "name": "imaging_contract_v1_marker",
            "table": {"selector": True},
        })
        self.assertEqual(plan["resource"], {
            "project": "imaging",
            "folder": "resources",
            "name": "imaging_contract_v1",
            "descriptor_name": "imaging.contract-v1",
            "url": (
                "https://armadillo.example.org/storage/projects/imaging/objects/"
                "markers%2Fimaging_contract_v1_marker.parquet"
            ),
            "format": "dsimaging-dataset:imaging.contract-v1",
        })
        self.assertEqual(plan["registry"], {
            "schema_version": 1,
            "imaging.contract-v1": {
                "enabled": True,
                "backend": "s3",
                "manifest_uri": (
                    "s3://site-images/datasets/imaging.contract-v1/manifest.yaml"
                ),
                "endpoint": "http://minio:9000",
                "credentials_ref": "imaging_store_ro",
                "region": "eu-west-1",
            },
        })
        self.assertEqual(len(plan["commands"]), 4)
        self.assertIn("armadillo.upload_table", plan["commands"][1])
        self.assertIn("resourcer::newResource", plan["commands"][2])
        self.assertIn("armadillo.upload_resource", plan["commands"][3])
        self.assertNotIn("legacy-admin-access", result.output)
        self.assertNotIn("legacy-admin-secret", result.output)
        self.assertNotIn("identity", plan["resource"])
        self.assertNotIn("secret", plan["resource"])

    def test_target_specific_options_fail_closed(self):
        result = CliRunner().invoke(main, [
            "--backend", "s3-compatible", "--endpoint", "https://s3.example.org",
            "dataset", "resource-plan", "study", "--target", "opal",
            "--project", "IMAGING",
            "--armadillo-url", "https://armadillo.example.org",
            "--credentials-ref", "read_only",
        ])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("only valid with --target armadillo", result.output)

        result = CliRunner().invoke(main, [
            "--backend", "s3-compatible", "--endpoint", "https://s3.example.org",
            "dataset", "resource-plan", "study", "--target", "armadillo",
            "--project", "imaging",
        ])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("requires --armadillo-url and --credentials-ref", result.output)

    @unittest.skipUnless(HAS_MOTO, "moto is not installed")
    def test_plan_validates_manifest_without_mutating_the_store(self):
        with mock_aws(), tempfile.TemporaryDirectory() as tmpdir:
            bucket = "imaging-data"
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=bucket)
            manifest = generate_manifest(
                "study", bucket, "datasets/study",
                privacy_unit_col="patient_id",
            )
            s3.put_object(
                Bucket=bucket,
                Key="datasets/study/manifest.yaml",
                Body=yaml.safe_dump(manifest).encode(),
            )
            before = s3.list_objects_v2(
                Bucket=bucket, Prefix="datasets/study/")["Contents"]
            with patch.dict(os.environ, {
                "DSIMAGING_CONFIG": os.path.join(tmpdir, "missing.yaml"),
            }):
                result = CliRunner().invoke(main, [
                    "--backend", "aws", "--bucket", bucket,
                    "dataset", "resource-plan", "study",
                    "--target", "opal", "--project", "IMAGING",
                ])
            after = s3.list_objects_v2(
                Bucket=bucket, Prefix="datasets/study/")["Contents"]

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(before, after)
        self.assertTrue(yaml.safe_load(result.output)["manifest"]["validated"])


if __name__ == "__main__":
    unittest.main()
