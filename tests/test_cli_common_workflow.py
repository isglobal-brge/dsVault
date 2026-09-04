import json
import os
import shutil
import stat
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

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
    _acquire_publish_lock,
    _hydrate_local_store_profile,
    _purge_dataset,
    main,
)
from dsimaging_admin.store import init_store


class CommonWorkflowTests(unittest.TestCase):
    def test_purge_postcondition_is_checked_before_releasing_its_lock(self):
        owned = {
            "key": "datasets/study/.publish-lock",
            "version_id": "lock-version",
        }
        old = {
            "key": "datasets/study/manifest.yaml",
            "version_id": "old-version",
        }
        with patch("dsimaging_admin.cli._require_bucket_versioning"), \
                patch("dsimaging_admin.cli._acquire_publish_lock",
                      return_value=owned), \
                patch("dsimaging_admin.cli.list_object_versions",
                      side_effect=[[owned, old], [owned]]) as versions, \
                patch("dsimaging_admin.cli.delete_object_versions",
                      return_value=1), \
                patch("dsimaging_admin.cli._release_publish_lock",
                      return_value=True):
            deleted = _purge_dataset(object(), "imaging-data", "study")

        self.assertEqual(deleted, 1)
        self.assertEqual(versions.call_count, 2)

    def test_cli_honours_dsimaging_config_without_creating_s3_client(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "config.yaml")
            Path(config_path).write_text(
                "default_profile: lab\nprofiles:\n  lab:\n    backend: aws\n",
                encoding="utf-8",
            )
            with patch.dict(os.environ, {"DSIMAGING_CONFIG": config_path}), \
                    patch("dsimaging_admin.cli.create_client") as create:
                result = CliRunner().invoke(
                    main, ["profile", "list", "--output", "json"])

            self.assertEqual(result.exit_code, 0, result.output)
            self.assertEqual(json.loads(result.output)["profiles"][0]["name"], "lab")
            create.assert_not_called()

    def test_profile_commands_ignore_incomplete_connection_environment(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "config.yaml")
            with patch.dict(os.environ, {
                "DSIMAGING_CONFIG": config_path,
                "DSIMAGING_BACKEND": "s3-compatible",
                "DSIMAGING_ENDPOINT": "",
            }):
                result = CliRunner().invoke(main, [
                    "profile", "add", "remote",
                    "--backend", "s3-compatible",
                    "--endpoint", "https://s3.example.org",
                ])

            self.assertEqual(result.exit_code, 0, result.output)
            config = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
            self.assertEqual(
                config["profiles"]["remote"]["endpoint"],
                "https://s3.example.org",
            )

    def test_explicit_store_lifecycle_ignores_incomplete_connection_environment(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = init_store(os.path.join(tmpdir, "local"))
            with patch.dict(os.environ, {
                "DSIMAGING_CONFIG": os.path.join(tmpdir, "missing.yaml"),
                "DSIMAGING_BACKEND": "s3-compatible",
                "DSIMAGING_ENDPOINT": "",
            }), patch("dsimaging_admin.cli.compose_ps",
                      return_value="healthy") as compose:
                result = CliRunner().invoke(
                    main, ["store", "ps", store.path])

            self.assertEqual(result.exit_code, 0, result.output)
            compose.assert_called_once_with(store.path)

    def test_setup_is_idempotent_and_stores_only_a_local_pointer(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = os.path.join(tmpdir, "study-store")
            config_path = os.path.join(tmpdir, "config.yaml")
            ready = {
                "ok": True, "docker": {"ok": True},
                "controller": {"ok": True}, "s3": {"ok": True},
            }
            runner = CliRunner()
            with patch.dict(os.environ, {"DSIMAGING_CONFIG": config_path}), \
                    patch("dsimaging_admin.cli.check_compose_prerequisites"), \
                    patch("dsimaging_admin.cli.compose_up", return_value="started"), \
                    patch("dsimaging_admin.cli.store_doctor", return_value=ready), \
                    patch("dsimaging_admin.cli.create_client") as create:
                first = runner.invoke(main, ["store", "setup", store_path])
                env_path = os.path.join(store_path, ".env")
                before = Path(env_path).read_bytes()
                before_mode = stat.S_IMODE(os.stat(env_path).st_mode)
                second = runner.invoke(main, ["store", "setup", store_path])

            self.assertEqual(first.exit_code, 0, first.output)
            self.assertEqual(second.exit_code, 0, second.output)
            self.assertEqual(Path(env_path).read_bytes(), before)
            self.assertEqual(stat.S_IMODE(os.stat(env_path).st_mode), before_mode)
            config = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
            self.assertEqual(config["default_profile"], "study-store")
            self.assertEqual(config["profiles"]["study-store"], {
                "kind": "local-store",
                "store_path": str(Path(store_path).resolve()),
            })
            create.assert_not_called()

    def test_setup_refuses_a_partial_or_foreign_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = Path(tmpdir) / "partial"
            store_path.mkdir()
            sentinel = store_path / "do-not-touch"
            sentinel.write_text("private", encoding="utf-8")
            with patch("dsimaging_admin.cli.check_compose_prerequisites"):
                result = CliRunner().invoke(
                    main, ["store", "setup", str(store_path)])

            self.assertNotEqual(result.exit_code, 0)
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "private")
            self.assertFalse((store_path / ".env").exists())

    def test_setup_checks_compose_before_generating_project(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = Path(tmpdir) / "not-created"
            with patch(
                "dsimaging_admin.cli.check_compose_prerequisites",
                side_effect=RuntimeError("compose unavailable"),
            ), patch("dsimaging_admin.cli.init_store") as initialize:
                result = CliRunner().invoke(
                    main, ["store", "setup", str(store_path)])

            self.assertNotEqual(result.exit_code, 0)
            self.assertIn("prerequisite check failed", result.output)
            initialize.assert_not_called()
            self.assertFalse(store_path.exists())

    def test_pathless_lifecycle_uses_active_local_store_and_is_lazy(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = init_store(os.path.join(tmpdir, "local"))
            config_path = os.path.join(tmpdir, "config.yaml")
            Path(config_path).write_text(yaml.safe_dump({
                "default_profile": "local",
                "profiles": {"local": {
                    "kind": "local-store", "store_path": store.path,
                }},
            }), encoding="utf-8")
            with patch.dict(os.environ, {"DSIMAGING_CONFIG": config_path}), \
                    patch("dsimaging_admin.cli.compose_ps", return_value="healthy") as ps, \
                    patch("dsimaging_admin.cli.create_client") as create:
                result = CliRunner().invoke(main, ["store", "ps"])

            self.assertEqual(result.exit_code, 0, result.output)
            ps.assert_called_once_with(store.path)
            create.assert_not_called()

    def test_explicit_lifecycle_path_bypasses_broken_active_local_profile(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            valid = init_store(os.path.join(tmpdir, "valid"))
            config_path = os.path.join(tmpdir, "config.yaml")
            Path(config_path).write_text(yaml.safe_dump({
                "default_profile": "missing",
                "profiles": {"missing": {
                    "kind": "local-store",
                    "store_path": os.path.join(tmpdir, "does-not-exist"),
                }},
            }), encoding="utf-8")
            with patch.dict(os.environ, {"DSIMAGING_CONFIG": config_path}), \
                    patch("dsimaging_admin.cli.compose_ps",
                          return_value="healthy") as compose:
                result = CliRunner().invoke(
                    main, ["store", "ps", valid.path])

            self.assertEqual(result.exit_code, 0, result.output)
            compose.assert_called_once_with(valid.path)

    def test_explicit_remote_connection_bypasses_broken_local_pointer(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "config.yaml")
            Path(config_path).write_text(yaml.safe_dump({
                "default_profile": "missing",
                "profiles": {"missing": {
                    "kind": "local-store",
                    "store_path": os.path.join(tmpdir, "does-not-exist"),
                }},
            }), encoding="utf-8")
            with patch.dict(os.environ, {"DSIMAGING_CONFIG": config_path}), \
                    patch("dsimaging_admin.cli.create_client",
                          return_value=object()) as create, \
                    patch("dsimaging_admin.cli.list_datasets", return_value=[]):
                result = CliRunner().invoke(main, [
                    "--backend", "s3-compatible",
                    "--endpoint", "https://s3.example.org",
                    "--bucket", "remote-bucket",
                    "dataset", "list",
                ])

            self.assertEqual(result.exit_code, 0, result.output)
            create.assert_called_once_with(
                "https://s3.example.org", None, None, "")

    def test_broken_local_pointer_does_not_fall_back_to_default_s3(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "config.yaml")
            Path(config_path).write_text(yaml.safe_dump({
                "default_profile": "missing",
                "profiles": {"missing": {
                    "kind": "local-store",
                    "store_path": os.path.join(tmpdir, "does-not-exist"),
                }},
            }), encoding="utf-8")
            with patch.dict(os.environ, {"DSIMAGING_CONFIG": config_path}), \
                    patch("dsimaging_admin.cli.create_client") as create:
                result = CliRunner().invoke(main, ["dataset", "list"])

            self.assertNotEqual(result.exit_code, 0)
            self.assertIn("Could not load local store project", result.output)
            create.assert_not_called()

    def test_broken_local_pointer_never_sends_stored_secrets_to_remote_s3(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "config.yaml")
            Path(config_path).write_text(yaml.safe_dump({
                "default_profile": "missing",
                "profiles": {"missing": {
                    "kind": "local-store",
                    "store_path": os.path.join(tmpdir, "does-not-exist"),
                    "access_key": "local-access",
                    "secret_key": "local-secret",
                }},
            }), encoding="utf-8")
            with patch.dict(os.environ, {"DSIMAGING_CONFIG": config_path}), \
                    patch("dsimaging_admin.cli.create_client",
                          return_value=object()) as create, \
                    patch("dsimaging_admin.cli.list_datasets", return_value=[]):
                result = CliRunner().invoke(main, [
                    "--backend", "s3-compatible",
                    "--endpoint", "https://s3.example.org",
                    "dataset", "list", "--skip-controller",
                ])

            self.assertEqual(result.exit_code, 0, result.output)
            create.assert_called_once_with(
                "https://s3.example.org", None, None, "")

    def test_reconcile_override_bypasses_broken_local_pointer(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "config.yaml")
            Path(config_path).write_text(yaml.safe_dump({
                "default_profile": "missing",
                "profiles": {"missing": {
                    "kind": "local-store",
                    "store_path": os.path.join(tmpdir, "does-not-exist"),
                }},
            }), encoding="utf-8")
            with patch.dict(os.environ, {
                "DSIMAGING_CONFIG": config_path,
                "DSIMAGING_CONTROLLER_TOKEN": "",
            }), patch("dsimaging_admin.cli.controller_api.reconcile",
                      return_value={"status": "queued"}) as reconcile:
                result = CliRunner().invoke(main, [
                    "dataset", "reconcile", "study", "--controller-url",
                    "https://controller.example.org",
                ])

            self.assertEqual(result.exit_code, 0, result.output)
            reconcile.assert_called_once_with(
                "https://controller.example.org", "study", token=None)

    def test_broken_local_pointer_never_sends_stored_token_to_remote_controller(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "config.yaml")
            Path(config_path).write_text(yaml.safe_dump({
                "default_profile": "missing",
                "profiles": {"missing": {
                    "kind": "local-store",
                    "store_path": os.path.join(tmpdir, "does-not-exist"),
                    "controller_token": "local-controller-token",
                }},
            }), encoding="utf-8")
            with patch.dict(os.environ, {
                "DSIMAGING_CONFIG": config_path,
                "DSIMAGING_CONTROLLER_TOKEN": "",
            }), patch("dsimaging_admin.cli.controller_api.reconcile",
                      return_value={"status": "queued"}) as reconcile:
                result = CliRunner().invoke(main, [
                    "--controller-url", "https://controller.example.org",
                    "dataset", "reconcile", "study",
                ])

            self.assertEqual(result.exit_code, 0, result.output)
            reconcile.assert_called_once_with(
                "https://controller.example.org", "study", token=None)

    def test_overridden_local_endpoint_does_not_reuse_local_credentials(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = init_store(os.path.join(tmpdir, "local"))
            config_path = os.path.join(tmpdir, "config.yaml")
            Path(config_path).write_text(yaml.safe_dump({
                "default_profile": "local",
                "profiles": {"local": {
                    "kind": "local-store", "store_path": store.path,
                }},
            }), encoding="utf-8")
            fake_client = object()
            with patch.dict(os.environ, {
                "DSIMAGING_CONFIG": config_path,
                "DSIMAGING_ENDPOINT": "https://s3.example.org",
            }), patch("dsimaging_admin.cli.create_client",
                      return_value=fake_client) as create, \
                    patch("dsimaging_admin.cli.list_datasets", return_value=[]):
                result = CliRunner().invoke(main, ["dataset", "list"])

            self.assertEqual(result.exit_code, 0, result.output)
            create.assert_called_once_with(
                "https://s3.example.org", None, None, "")

    def test_matching_local_endpoint_keeps_local_credentials(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = init_store(os.path.join(tmpdir, "local"))
            config_path = os.path.join(tmpdir, "config.yaml")
            Path(config_path).write_text(yaml.safe_dump({
                "default_profile": "local",
                "profiles": {"local": {
                    "kind": "local-store", "store_path": store.path,
                }},
            }), encoding="utf-8")
            with patch.dict(os.environ, {"DSIMAGING_CONFIG": config_path}), \
                    patch("dsimaging_admin.cli.create_client",
                          return_value=object()) as create, \
                    patch("dsimaging_admin.cli.list_datasets", return_value=[]):
                result = CliRunner().invoke(main, [
                    "--endpoint", store.endpoint, "dataset", "list",
                ])

            self.assertEqual(result.exit_code, 0, result.output)
            create.assert_called_once_with(
                store.endpoint, store.access_key, store.secret_key, "")

    def test_local_profile_cannot_forge_credential_scope_sentinels(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = init_store(os.path.join(tmpdir, "local"))
            profile = _hydrate_local_store_profile({
                "kind": "local-store",
                "store_path": store.path,
                "endpoint": "https://remote.example.org",
                "controller_url": "https://controller.example.org",
                "_local_endpoint": "https://remote.example.org",
                "_local_controller_url": "https://controller.example.org",
            })

        self.assertEqual(profile["_local_endpoint"], store.endpoint)
        self.assertEqual(
            profile["_local_controller_url"], store.controller_url)

    def test_overridden_controller_does_not_reuse_local_operator_token(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = init_store(os.path.join(tmpdir, "local"))
            config_path = os.path.join(tmpdir, "config.yaml")
            Path(config_path).write_text(yaml.safe_dump({
                "default_profile": "local",
                "profiles": {"local": {
                    "kind": "local-store", "store_path": store.path,
                }},
            }), encoding="utf-8")
            with patch.dict(os.environ, {
                "DSIMAGING_CONFIG": config_path,
                "DSIMAGING_CONTROLLER_TOKEN": "",
            }), patch("dsimaging_admin.cli.controller_api.reconcile",
                      return_value={"status": "queued"}) as reconcile:
                result = CliRunner().invoke(main, [
                    "--controller-url", "https://controller.example.org",
                    "dataset", "reconcile", "study",
                ])

            self.assertEqual(result.exit_code, 0, result.output)
            reconcile.assert_called_once_with(
                "https://controller.example.org", "study", token=None)

    def test_subcommand_controller_override_drops_local_operator_token(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = init_store(os.path.join(tmpdir, "local"))
            config_path = os.path.join(tmpdir, "config.yaml")
            Path(config_path).write_text(yaml.safe_dump({
                "default_profile": "local",
                "profiles": {"local": {
                    "kind": "local-store", "store_path": store.path,
                }},
            }), encoding="utf-8")
            with patch.dict(os.environ, {
                "DSIMAGING_CONFIG": config_path,
                "DSIMAGING_CONTROLLER_TOKEN": "",
            }), patch("dsimaging_admin.cli.controller_api.reconcile",
                      return_value={"status": "queued"}) as reconcile:
                result = CliRunner().invoke(main, [
                    "dataset", "reconcile", "study",
                    "--controller-url", "https://controller.example.org",
                ])

            self.assertEqual(result.exit_code, 0, result.output)
            reconcile.assert_called_once_with(
                "https://controller.example.org", "study", token=None)

    def test_explicit_token_can_pair_with_subcommand_controller_override(self):
        with patch("dsimaging_admin.cli.controller_api.reconcile",
                   return_value={"status": "queued"}) as reconcile:
            result = CliRunner().invoke(main, [
                "--controller-token", "explicit-token",
                "dataset", "reconcile", "study",
                "--controller-url", "https://controller.example.org",
            ])

        self.assertEqual(result.exit_code, 0, result.output)
        reconcile.assert_called_once_with(
            "https://controller.example.org", "study", token="explicit-token")

    def test_subcommand_controller_override_rejects_embedded_secret(self):
        with patch("dsimaging_admin.cli.controller_api.reconcile") as reconcile:
            result = CliRunner().invoke(main, [
                "dataset", "reconcile", "study", "--controller-url",
                "https://user:private@controller.example.org",
            ])

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("without credentials", result.output)
        self.assertNotIn("user:private", result.output)
        reconcile.assert_not_called()

    def test_volume_removal_requires_the_second_confirmation_flag(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = init_store(os.path.join(tmpdir, "local"))
            with patch("dsimaging_admin.cli.compose_down") as down:
                result = CliRunner().invoke(
                    main, ["store", "down", store.path, "--volumes"])
            self.assertNotEqual(result.exit_code, 0)
            self.assertIn("--volumes and --yes", result.output)
            down.assert_not_called()

    def test_profile_show_redacts_legacy_secrets(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "config.yaml")
            Path(config_path).write_text(yaml.safe_dump({
                "default_profile": "legacy",
                "profiles": {"legacy": {
                    "endpoint": "https://s3.example.org",
                    "access_key": "must-not-print",
                    "secret_key": "also-private",
                    "controller_token": "private-token",
                }},
            }), encoding="utf-8")
            with patch.dict(os.environ, {"DSIMAGING_CONFIG": config_path}):
                result = CliRunner().invoke(
                    main, ["profile", "show", "legacy", "--output", "json"])

            self.assertEqual(result.exit_code, 0, result.output)
            self.assertNotIn("must-not-print", result.output)
            self.assertNotIn("also-private", result.output)
            self.assertNotIn("private-token", result.output)

    def test_profile_show_redacts_credentials_embedded_in_legacy_url(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "config.yaml")
            Path(config_path).write_text(yaml.safe_dump({
                "default_profile": "legacy",
                "profiles": {"legacy": {
                    "endpoint": "https://user:private@s3.example.org",
                    "controller_url": "https://controller.example.org?token=private",
                }},
            }), encoding="utf-8")
            with patch.dict(os.environ, {"DSIMAGING_CONFIG": config_path}):
                result = CliRunner().invoke(
                    main, ["profile", "show", "legacy", "--output", "json"])

            self.assertEqual(result.exit_code, 0, result.output)
            self.assertNotIn("private", result.output)
            self.assertEqual(result.output.count("<redacted URL>"), 2)

    def test_profile_add_rejects_endpoint_with_embedded_credentials(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "config.yaml")
            with patch.dict(os.environ, {"DSIMAGING_CONFIG": config_path}):
                result = CliRunner().invoke(main, [
                    "profile", "add", "unsafe",
                    "--backend", "s3-compatible",
                    "--endpoint", "https://user:private@s3.example.org",
                ])

            self.assertNotEqual(result.exit_code, 0)
            self.assertFalse(Path(config_path).exists())
            self.assertNotIn("user:private", result.output)

    def test_profile_add_rejects_controller_url_with_embedded_secret(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "config.yaml")
            with patch.dict(os.environ, {"DSIMAGING_CONFIG": config_path}):
                result = CliRunner().invoke(main, [
                    "profile", "add", "unsafe",
                    "--controller-url",
                    "https://controller.example.org?token=private",
                ])

            self.assertNotEqual(result.exit_code, 0)
            self.assertFalse(Path(config_path).exists())
            self.assertNotIn("token=private", result.output)

    def test_concurrent_profile_updates_preserve_both_writes(self):
        from dsimaging_admin import cli

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "config.yaml")
            entered = threading.Event()
            release = threading.Event()
            calls = []
            failures = []
            original_write = cli._write_config

            def controlled_write(data, path=None):
                calls.append(sorted((data.get("profiles") or {}).keys()))
                if len(calls) == 1:
                    entered.set()
                    release.wait(timeout=5)
                return original_write(data, path)

            def add_profile(name):
                try:
                    cli._write_config_profile(
                        name, {"backend": "aws"}, set_default=False)
                except Exception as exc:  # pragma: no cover - assertion below
                    failures.append(exc)

            with patch.dict(os.environ, {"DSIMAGING_CONFIG": config_path}), \
                    patch.object(cli, "_write_config",
                                 side_effect=controlled_write):
                first = threading.Thread(target=add_profile, args=("alpha",))
                second = threading.Thread(target=add_profile, args=("beta",))
                first.start()
                self.assertTrue(entered.wait(timeout=5))
                second.start()
                time.sleep(0.05)
                self.assertEqual(len(calls), 1)
                release.set()
                first.join(timeout=5)
                second.join(timeout=5)

            self.assertEqual(failures, [])
            config = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
            self.assertEqual(set(config["profiles"]), {"alpha", "beta"})

    def test_profile_add_use_and_list_do_not_persist_credentials(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "config.yaml")
            with patch.dict(os.environ, {"DSIMAGING_CONFIG": config_path}):
                runner = CliRunner()
                added = runner.invoke(main, [
                    "profile", "add", "aws-prod", "--backend", "aws",
                    "--bucket", "imaging-prod", "--region", "eu-west-1",
                ])
                listed = runner.invoke(
                    main, ["profile", "list", "--output", "json"])
                selected = runner.invoke(main, ["profile", "use", "aws-prod"])

            self.assertEqual(added.exit_code, 0, added.output)
            self.assertEqual(listed.exit_code, 0, listed.output)
            self.assertEqual(selected.exit_code, 0, selected.output)
            raw = Path(config_path).read_text(encoding="utf-8")
            self.assertNotIn("access_key", raw)
            self.assertNotIn("secret_key", raw)
            self.assertEqual(
                json.loads(listed.output)["profiles"][0]["name"], "aws-prod")

    def test_store_provision_requires_an_explicit_aws_selection(self):
        with patch("dsimaging_admin.cli.provision_aws_store") as provision:
            result = CliRunner().invoke(main, ["store", "provision"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("explicit AWS", result.output)
        provision.assert_not_called()

    def test_aws_provision_does_not_repurpose_local_store_profile(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = init_store(os.path.join(tmpdir, "local"))
            config_path = os.path.join(tmpdir, "config.yaml")
            original = {
                "default_profile": "local",
                "profiles": {"local": {
                    "kind": "local-store", "store_path": store.path,
                }},
            }
            Path(config_path).write_text(
                yaml.safe_dump(original), encoding="utf-8")
            with patch.dict(os.environ, {"DSIMAGING_CONFIG": config_path}), \
                    patch("dsimaging_admin.cli.provision_aws_store") as provision:
                result = CliRunner().invoke(main, [
                    "--backend", "aws", "store", "provision",
                ])

            self.assertNotEqual(result.exit_code, 0)
            self.assertIn("separate AWS profile", result.output)
            provision.assert_not_called()
            self.assertEqual(
                yaml.safe_load(Path(config_path).read_text(encoding="utf-8")),
                original,
            )

    def test_endpoint_rejects_embedded_credentials_before_client_creation(self):
        with patch("dsimaging_admin.cli.create_client") as create:
            result = CliRunner().invoke(main, [
                "--endpoint", "https://user:secret@s3.example.org",
                "dataset", "list",
            ])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("without credentials", result.output)
        self.assertNotIn("user:secret", result.output)
        create.assert_not_called()

    def test_s3_compatible_mode_requires_an_endpoint(self):
        with tempfile.TemporaryDirectory() as tmpdir, patch.dict(os.environ, {
            "DSIMAGING_CONFIG": os.path.join(tmpdir, "missing.yaml"),
            "DSIMAGING_ENDPOINT": "",
        }), patch("dsimaging_admin.cli.create_client") as create:
            result = CliRunner().invoke(main, [
                "--backend", "s3-compatible", "dataset", "list",
            ])

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("requires an explicit endpoint", result.output)
        create.assert_not_called()

    def test_ui_s3_compatible_mode_without_config_has_no_local_fallback(self):
        try:
            from dsimaging_admin import ui
        except ImportError:
            self.skipTest("streamlit is not installed")
        with tempfile.TemporaryDirectory() as tmpdir, patch.dict(os.environ, {
            "DSIMAGING_CONFIG": os.path.join(tmpdir, "missing.yaml"),
            "DSIMAGING_BACKEND": "s3-compatible",
            "DSIMAGING_ENDPOINT": "",
        }):
            profile = ui.load_profiles()["default"]

        self.assertEqual(profile["endpoint"], "")

    def test_local_ui_resolution_keeps_secrets_out_of_browser_fields(self):
        try:
            from dsimaging_admin import ui
        except ImportError:
            self.skipTest("streamlit is not installed")
        with tempfile.TemporaryDirectory() as tmpdir:
            store = init_store(os.path.join(tmpdir, "local"))
            profile = _hydrate_local_store_profile({
                "kind": "local-store", "store_path": store.path,
            })

            def text_input(label, value="", **kwargs):
                return value

            with patch.object(ui.st.sidebar, "text_input", side_effect=text_input), \
                    patch.object(ui.st.sidebar, "selectbox", return_value="minio"), \
                    patch.object(ui.st.sidebar, "subheader"), \
                    patch.object(ui.st.sidebar, "caption"), \
                    patch.object(ui.st.sidebar, "markdown") as markdown:
                resolved = ui.edit_connection_config(profile)

            self.assertEqual(resolved["access_key"], store.access_key)
            self.assertEqual(resolved["secret_key"], store.secret_key)
            rendered = str(markdown.call_args)
            self.assertNotIn(store.access_key, rendered)
            self.assertNotIn(store.secret_key, rendered)

            def changed_controller(label, value="", **kwargs):
                if label == "Controller URL":
                    return "https://controller.example.org"
                return value

            with patch.dict(os.environ, {"DSIMAGING_CONTROLLER_TOKEN": ""}), \
                    patch.object(ui.st.sidebar, "text_input",
                                 side_effect=changed_controller), \
                    patch.object(ui.st.sidebar, "selectbox", return_value="minio"), \
                    patch.object(ui.st.sidebar, "subheader"), \
                    patch.object(ui.st.sidebar, "caption"), \
                    patch.object(ui.st.sidebar, "markdown"):
                changed = ui.edit_connection_config(profile)
            self.assertEqual(changed["controller_token"], "")

    def test_aws_ui_profile_without_endpoint_uses_sdk_default(self):
        try:
            from dsimaging_admin import ui
        except ImportError:
            self.skipTest("streamlit is not installed")
        profile = {"backend": "aws", "bucket": "imaging-data"}

        def text_input(label, value="", **kwargs):
            return value

        with patch.object(ui.st.sidebar, "selectbox", return_value="aws"), \
                patch.object(ui.st.sidebar, "text_input", side_effect=text_input), \
                patch.object(ui.st.sidebar, "subheader"), \
                patch.object(ui.st.sidebar, "caption"), \
                patch.object(ui.st.sidebar, "markdown"):
            resolved = ui.edit_connection_config(profile)

        self.assertEqual(resolved["endpoint"], "")
        self.assertEqual(resolved["resolved_backend"], "aws")

    def test_ui_sqs_environment_value_overrides_profile(self):
        try:
            from dsimaging_admin import ui
        except ImportError:
            self.skipTest("streamlit is not installed")
        profile = {
            "backend": "aws",
            "bucket": "imaging-data",
            "aws": {"sqs_queue_url": "https://queue.example/profile"},
        }

        def text_input(label, value="", **kwargs):
            return value

        with patch.dict(os.environ, {
            "DSIMAGING_SQS_QUEUE_URL": "https://queue.example/environment",
        }), patch.object(ui.st.sidebar, "selectbox", return_value="aws"), \
                patch.object(ui.st.sidebar, "text_input", side_effect=text_input), \
                patch.object(ui.st.sidebar, "subheader"), \
                patch.object(ui.st.sidebar, "caption"), \
                patch.object(ui.st.sidebar, "markdown"):
            resolved = ui.edit_connection_config(profile)

        self.assertEqual(
            resolved["sqs_queue_url"],
            "https://queue.example/environment",
        )

    def test_ui_rejects_secret_bearing_urls_before_rendering_inputs(self):
        try:
            from dsimaging_admin import ui
        except ImportError:
            self.skipTest("streamlit is not installed")
        profile = {
            "backend": "s3-compatible",
            "endpoint": "https://user:private@s3.example.org",
        }
        with patch.object(ui.st.sidebar, "selectbox",
                          return_value="s3-compatible"), \
                patch.object(ui.st.sidebar, "text_input") as text_input, \
                patch.object(ui.st.sidebar, "subheader"):
            with self.assertRaisesRegex(ValueError, "without credentials"):
                ui.edit_connection_config(profile)

        text_input.assert_not_called()

    def test_ui_remote_override_bypasses_broken_local_pointer(self):
        try:
            from dsimaging_admin import ui
        except ImportError:
            self.skipTest("streamlit is not installed")
        profile = {
            "kind": "local-store", "store_path": "/does/not/exist",
        }
        with patch.dict(os.environ, {
            "DSIMAGING_BACKEND": "s3-compatible",
            "DSIMAGING_ENDPOINT": "https://s3.example.org",
        }):
            hydrated = ui._hydrate_connection_profile(profile)

        self.assertEqual(hydrated, profile)
        self.assertNotIn("access_key", hydrated)

    def test_publication_preflight_detects_local_content_change(self):
        try:
            from dsimaging_admin import ui
        except ImportError:
            self.skipTest("streamlit is not installed")
        fixture = Path(__file__).parent / "fixtures" / "tiny_dataset"
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "dataset"
            shutil.copytree(fixture, source)
            expected = ui.publication_preflight(
                "study", str(source), None, None, None, [])
            image = source / "source" / "images" / "case001.nii.gz"
            image.write_bytes(image.read_bytes() + b"changed")

            with self.assertRaisesRegex(ValueError, "changed after Preview"):
                ui.ensure_publication_preflight_current(
                    expected, "study", str(source), None, None, None, [])


@unittest.skipUnless(HAS_MOTO, "moto is not installed")
class VersionedDatasetWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.mock = mock_aws()
        self.mock.start()
        self.bucket = "imaging-data"
        self.s3 = boto3.client("s3", region_name="us-east-1")
        self.s3.create_bucket(Bucket=self.bucket)
        self.s3.put_bucket_versioning(
            Bucket=self.bucket,
            VersioningConfiguration={"Status": "Enabled"},
        )

    def tearDown(self):
        self.mock.stop()

    def _source(self, root: str, *, two_metadata_files: bool = False,
                patient_column: str = "patient_id") -> str:
        source = Path(root) / "source"
        images = source / "images"
        images.mkdir(parents=True)
        (images / "case001.nii.gz").write_bytes(b"nifti")
        (source / "metadata.csv").write_text(
            f"sample_id,{patient_column}\ncase001,patient-a\n",
            encoding="utf-8",
        )
        if two_metadata_files:
            import pyarrow as pa
            import pyarrow.parquet as pq
            pq.write_table(pa.table({
                "sample_id": ["case001"], patient_column: ["patient-a"],
            }), source / "metadata.parquet")
        return str(source)

    def test_positional_publish_infers_metadata_and_verifies_before_commit(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = self._source(tmpdir)
            result = CliRunner().invoke(main, [
                "--backend", "aws", "--bucket", self.bucket,
                "dataset", "publish", "study", source,
                "--skip-dicom-checks",
            ])

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("Verification: quick", result.output)
        self.s3.head_object(
            Bucket=self.bucket, Key="datasets/study/manifest.yaml")
        current = self.s3.list_objects_v2(
            Bucket=self.bucket, Prefix="datasets/study/.publish-lock")
        self.assertEqual(current.get("Contents", []), [])
        history = self.s3.list_object_versions(
            Bucket=self.bucket, Prefix="datasets/study/")
        historical_keys = {
            item["Key"] for item in (
                history.get("Versions", []) + history.get("DeleteMarkers", []))
        }
        self.assertFalse(any(
            "/.staging-" in key or "/.backup-" in key
            for key in historical_keys
        ))

    def test_ambiguous_metadata_fails_before_s3_client_creation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = self._source(tmpdir, two_metadata_files=True)
            with patch("dsimaging_admin.cli.create_client") as create:
                result = CliRunner().invoke(main, [
                    "--backend", "aws", "--bucket", self.bucket,
                    "dataset", "publish", "study", source,
                    "--skip-dicom-checks",
                ])

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Both metadata.csv and metadata.parquet", result.output)
        create.assert_not_called()

    def test_failed_precommit_verification_rolls_back(self):
        issue = Mock(detail="forced drift")
        verification = Mock(ok=False, issues=[issue])
        with tempfile.TemporaryDirectory() as tmpdir:
            source = self._source(tmpdir)
            with patch("dsimaging_admin.cli.verify_dataset",
                       return_value=verification):
                result = CliRunner().invoke(main, [
                    "--backend", "aws", "--bucket", self.bucket,
                    "dataset", "publish", "study", source,
                    "--skip-dicom-checks",
                ])

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("verification failed", str(result.exception))
        objects = self.s3.list_objects_v2(
            Bucket=self.bucket, Prefix="datasets/study/").get("Contents", [])
        self.assertEqual(objects, [])

    def test_local_file_change_during_upload_rolls_back(self):
        from dsimaging_admin import cli

        with tempfile.TemporaryDirectory() as tmpdir:
            source = self._source(tmpdir)
            original_upload = cli._upload_sample
            changed = False

            def changing_upload(s3, bucket, prefix, sample, source_path):
                nonlocal changed
                if source_path == "images" and not changed:
                    Path(sample["local_path"]).write_bytes(b"changed-during-upload")
                    changed = True
                return original_upload(
                    s3, bucket, prefix, sample, source_path)

            with patch.object(cli, "_upload_sample",
                              side_effect=changing_upload):
                result = CliRunner().invoke(main, [
                    "--backend", "aws", "--bucket", self.bucket,
                    "dataset", "publish", "study", source,
                    "--skip-dicom-checks",
                ])

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("changed while it was being uploaded", str(result.exception))
        objects = self.s3.list_objects_v2(
            Bucket=self.bucket, Prefix="datasets/study/").get("Contents", [])
        self.assertEqual(objects, [])

    def test_delete_preserves_history_then_purge_removes_every_version(self):
        key = "datasets/study/manifest.yaml"
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=b"first")
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=b"second")
        runner = CliRunner()

        deleted = runner.invoke(main, [
            "--backend", "aws", "--bucket", self.bucket,
            "dataset", "delete", "study", "--yes",
        ])
        self.assertEqual(deleted.exit_code, 0, deleted.output)
        history = self.s3.list_object_versions(
            Bucket=self.bucket, Prefix="datasets/study/")
        self.assertTrue(history.get("Versions") or history.get("DeleteMarkers"))

        refused = runner.invoke(main, [
            "--backend", "aws", "--bucket", self.bucket,
            "dataset", "purge", "study", "--yes",
        ])
        self.assertNotEqual(refused.exit_code, 0)

        purged = runner.invoke(main, [
            "--backend", "aws", "--bucket", self.bucket,
            "dataset", "purge", "study", "--yes", "--confirm", "study",
        ])
        self.assertEqual(purged.exit_code, 0, purged.output)
        history = self.s3.list_object_versions(
            Bucket=self.bucket, Prefix="datasets/study/")
        self.assertEqual(
            history.get("Versions", []) + history.get("DeleteMarkers", []), [])

    def test_delete_refuses_to_claim_history_preservation_without_versioning(self):
        self.s3.put_bucket_versioning(
            Bucket=self.bucket,
            VersioningConfiguration={"Status": "Suspended"},
        )
        key = "datasets/unversioned/manifest.yaml"
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=b"only-copy")

        result = CliRunner().invoke(main, [
            "--backend", "aws", "--bucket", self.bucket,
            "dataset", "delete", "unversioned", "--yes",
        ])

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("requires bucket versioning", result.output)
        self.assertEqual(
            self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read(),
            b"only-copy",
        )

    def test_expected_publish_lock_rejects_a_replaced_lock_version(self):
        from dsimaging_admin.verify import _validate_publish_lock

        prefix = "datasets/study"
        lock = _acquire_publish_lock(
            self.s3, self.bucket, prefix, status="publishing")
        _validate_publish_lock(
            self.s3, self.bucket, f"{prefix}/.publish-lock", lock)
        self.s3.put_object(
            Bucket=self.bucket,
            Key=f"{prefix}/.publish-lock",
            Body=json.dumps({"owner": lock["owner"]}).encode("utf-8"),
        )
        with self.assertRaisesRegex(ValueError, "ETag|version"):
            _validate_publish_lock(
                self.s3, self.bucket, f"{prefix}/.publish-lock", lock)


if __name__ == "__main__":
    unittest.main()
