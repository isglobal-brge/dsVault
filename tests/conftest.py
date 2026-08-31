"""Make the test suite hermetic: never touch the invoking user's real HOME.

This module is imported by pytest before any test module, so the HOME
override below takes effect before ``dsimaging_admin.cli`` computes its
import-time ``CONFIG_PATH``. The explicit ``CONFIG_PATH`` patch afterwards
is belt and braces (and also covers a cli module imported earlier).
"""

import os
import tempfile

_HERMETIC_HOME = tempfile.mkdtemp(prefix="dsimaging-admin-tests-home-")
os.environ["HOME"] = _HERMETIC_HOME
os.environ["USERPROFILE"] = _HERMETIC_HOME

import dsimaging_admin.cli as _cli  # noqa: E402

_cli.CONFIG_PATH = os.path.join(_HERMETIC_HOME, ".dsimaging.yaml")
