import os
import stat
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

from click.testing import CliRunner

from dsimaging_admin.cli import (
    _persist_aws_queue_url,
    _write_config_profile,
    main,
)


class UiCliTests(unittest.TestCase):
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

    def test_ui_group_without_subcommand_launches_dashboard(self):
        calls = []
        fake_ui = types.ModuleType("dsimaging_admin.ui")

        def launch_ui(host, port, open_browser):
            calls.append((host, port, open_browser))

        fake_ui.launch_ui = launch_ui
        with patch.dict(sys.modules, {"dsimaging_admin.ui": fake_ui}):
            result = CliRunner().invoke(main, ["ui"])

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(calls, [("127.0.0.1", 8501, False)])

    def test_ui_group_refuses_non_loopback_bind(self):
        calls = []
        fake_ui = types.ModuleType("dsimaging_admin.ui")

        def launch_ui(host, port, open_browser):
            calls.append((host, port, open_browser))

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
