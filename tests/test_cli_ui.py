import sys
import types
import unittest
from unittest.mock import patch

from click.testing import CliRunner

from dsimaging_admin.cli import main


class UiCliTests(unittest.TestCase):
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

    def test_ui_group_accepts_launch_options_without_subcommand(self):
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

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(calls, [("0.0.0.0", 8600, True)])
        self.assertIn("WARNING: dsimaging-admin ui is an unauthenticated", result.stderr)


if __name__ == "__main__":
    unittest.main()
