import os
import stat
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

from click.testing import CliRunner

from dsimaging_admin.cli import (
    _load_profile,
    _persist_aws_queue_url,
    _write_config_profile,
    main,
)


class UiCliTests(unittest.TestCase):
    def test_configured_default_profile_is_used(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "config.yaml")
            with open(config_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "default_profile: remote\n"
                    "profiles:\n"
                    "  default:\n"
                    "    endpoint: http://127.0.0.1:9000\n"
                    "  remote:\n"
                    "    endpoint: https://store.example.org\n"
                )
            with patch("dsimaging_admin.cli.CONFIG_PATH", config_path):
                selected, config = _load_profile()

        self.assertEqual(selected, "remote")
        self.assertEqual(config["endpoint"], "https://store.example.org")

    def test_unknown_and_invalid_profiles_fail_before_client_creation(self):
        fixtures = {
            "unknown": (
                "default_profile: missing\nprofiles:\n  present: {}\n",
                "does not exist",
            ),
            "invalid": ("profiles: [broken\n", "Could not read"),
            "invalid-aws": (
                "profiles:\n  default:\n    aws: invalid\n",
                "invalid AWS section",
            ),
        }
        for name, (content, expected) in fixtures.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmpdir:
                config_path = os.path.join(tmpdir, "config.yaml")
                with open(config_path, "w", encoding="utf-8") as handle:
                    handle.write(content)
                with patch("dsimaging_admin.cli.CONFIG_PATH", config_path), \
                        patch("dsimaging_admin.cli.create_client") as create:
                    result = CliRunner().invoke(main, ["doctor"])

                self.assertNotEqual(result.exit_code, 0)
                self.assertIn(expected, result.output)
                create.assert_not_called()

    def test_explicit_profile_without_config_fails_before_client_creation(self):
        with tempfile.TemporaryDirectory() as tmpdir, \
                patch("dsimaging_admin.cli.CONFIG_PATH",
                      os.path.join(tmpdir, "missing.yaml")), \
                patch("dsimaging_admin.cli.create_client") as create:
            result = CliRunner().invoke(main, ["--profile", "production", "doctor"])

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("does not exist", result.output)
        create.assert_not_called()

    def test_explicit_default_without_config_uses_builtin_defaults(self):
        with tempfile.TemporaryDirectory() as tmpdir, patch(
            "dsimaging_admin.cli.CONFIG_PATH",
            os.path.join(tmpdir, "missing.yaml"),
        ):
            selected, config = _load_profile("default")

        self.assertEqual(selected, "default")
        self.assertEqual(config, {})

    def test_aws_mode_rejects_custom_endpoint_before_client_creation(self):
        with tempfile.TemporaryDirectory() as tmpdir, \
                patch("dsimaging_admin.cli.CONFIG_PATH",
                      os.path.join(tmpdir, "missing.yaml")), \
                patch("dsimaging_admin.cli.create_client") as create:
            result = CliRunner().invoke(main, [
                "--backend", "aws",
                "--endpoint", "https://s3.amazonaws.com.attacker.invalid",
                "doctor",
            ])

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("native HTTPS S3 endpoint", result.output)
        create.assert_not_called()

    def test_config_is_private_before_post_write_chmod(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "config.yaml")
            with patch("dsimaging_admin.cli.CONFIG_PATH", config_path), \
                    patch("dsimaging_admin.cli.os.chmod",
                          side_effect=OSError("simulated interruption")):
                with self.assertRaises(OSError):
                    _write_config_profile("default", {"secret_key": "private"})

            mode = stat.S_IMODE(os.stat(config_path).st_mode)
            self.assertEqual(mode, 0o600)

    def test_aws_queue_config_write_preserves_previous_file_on_failure(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "config.yaml")
            original = "profiles:\n  default:\n    secret_key: private\n"
            with open(config_path, "w", encoding="utf-8") as handle:
                handle.write(original)
            os.chmod(config_path, 0o600)

            with patch("dsimaging_admin.cli.CONFIG_PATH", config_path), \
                    patch("yaml.dump", side_effect=OSError("interrupted write")):
                with self.assertRaises(OSError):
                    _persist_aws_queue_url("default", "https://queue.example")

            with open(config_path, encoding="utf-8") as handle:
                self.assertEqual(handle.read(), original)

    def test_aws_queue_config_normalizes_an_empty_profile(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "config.yaml")
            with open(config_path, "w", encoding="utf-8") as handle:
                handle.write("default_profile: empty\nprofiles:\n  empty:\n")

            _persist_aws_queue_url(
                "empty", "https://queue.example", config_path=config_path)

            import yaml
            with open(config_path, encoding="utf-8") as handle:
                config = yaml.safe_load(handle)
        self.assertEqual(config["profiles"]["empty"]["backend"], "aws")
        self.assertEqual(
            config["profiles"]["empty"]["aws"]["sqs_queue_url"],
            "https://queue.example",
        )

    def test_aws_queue_config_normalizes_an_empty_aws_section(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "config.yaml")
            with open(config_path, "w", encoding="utf-8") as handle:
                handle.write("profiles:\n  default:\n    aws:\n")

            _persist_aws_queue_url(
                "default", "https://queue.example", config_path=config_path)

            import yaml
            with open(config_path, encoding="utf-8") as handle:
                config = yaml.safe_load(handle)
        self.assertEqual(
            config["profiles"]["default"]["aws"]["sqs_queue_url"],
            "https://queue.example",
        )

    def test_ui_group_without_subcommand_launches_dashboard(self):
        calls = []
        fake_ui = types.ModuleType("dsimaging_admin.ui")

        def launch_ui(host, port, open_browser, environment):
            calls.append((host, port, open_browser, environment))

        fake_ui.launch_ui = launch_ui
        with patch.dict(sys.modules, {"dsimaging_admin.ui": fake_ui}):
            result = CliRunner().invoke(main, ["ui"])

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(calls[0][:3], ("127.0.0.1", 8501, False))
        self.assertNotIn("DSIMAGING_ACCESS_KEY", calls[0][3])
        self.assertNotIn("DSIMAGING_SECRET_KEY", calls[0][3])
        self.assertNotIn("DSIMAGING_CONTROLLER_TOKEN", calls[0][3])
        self.assertNotIn("DSIMAGING_ENDPOINT", calls[0][3])

    def test_ui_group_refuses_non_loopback_bind(self):
        calls = []
        fake_ui = types.ModuleType("dsimaging_admin.ui")

        def launch_ui(host, port, open_browser, environment):
            calls.append((host, port, open_browser, environment))

        fake_ui.launch_ui = launch_ui
        with patch.dict(sys.modules, {"dsimaging_admin.ui": fake_ui}):
            result = CliRunner().invoke(main, [
                "ui",
                "--host", "0.0.0.0",
                "--port", "8600",
                "--open-browser",
            ])

        self.assertNotEqual(result.exit_code, 0, result.output)
        self.assertEqual(calls, [])
        self.assertIn("may only bind to loopback", result.output)


if __name__ == "__main__":
    unittest.main()
