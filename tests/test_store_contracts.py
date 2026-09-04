import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from dsimaging_admin.store import (
    compose_down,
    compose_up,
    init_store,
    load_store_config,
    store_doctor,
)


class StoreConfigContractTests(unittest.TestCase):
    def test_mutating_compose_operations_are_serialized_per_project(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            barrier = threading.Barrier(2)
            entered = threading.Event()
            release = threading.Event()
            state_lock = threading.Lock()
            active = 0
            max_active = 0

            def run(*_args, **_kwargs):
                nonlocal active, max_active
                with state_lock:
                    active += 1
                    max_active = max(max_active, active)
                entered.set()
                release.wait(timeout=5)
                with state_lock:
                    active -= 1
                return "ok"

            def operation(function):
                barrier.wait(timeout=5)
                function(tmpdir)

            with patch("dsimaging_admin.store.shutil.which", return_value="docker"), \
                    patch("dsimaging_admin.store._run", side_effect=run):
                threads = [
                    threading.Thread(target=operation, args=(compose_up,)),
                    threading.Thread(target=operation, args=(compose_down,)),
                ]
                for thread in threads:
                    thread.start()
                self.assertTrue(entered.wait(timeout=5))
                release.set()
                for thread in threads:
                    thread.join(timeout=5)

            self.assertFalse(any(thread.is_alive() for thread in threads))
            self.assertEqual(max_active, 1)

    def test_concurrent_initialization_has_one_consistent_winner(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            project = str(Path(tmpdir) / "store")
            barrier = threading.Barrier(2)
            successes = []
            failures = []

            def initialize(name):
                barrier.wait(timeout=5)
                try:
                    successes.append(init_store(
                        project,
                        access_key=f"{name}-access",
                        secret_key=f"{name}-secret-value",
                    ))
                except Exception as exc:
                    failures.append(exc)

            threads = [
                threading.Thread(target=initialize, args=(name,))
                for name in ("alpha", "beta")
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=5)

            self.assertEqual(len(successes), 1)
            self.assertEqual(len(failures), 1)
            self.assertIsInstance(failures[0], FileExistsError)
            loaded = load_store_config(project)
            self.assertEqual(loaded.access_key, successes[0].access_key)
            self.assertEqual(loaded.secret_key, successes[0].secret_key)

    def test_load_rejects_empty_and_partial_projects(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaisesRegex(ValueError, "complete generated"):
                load_store_config(tmpdir)

            (Path(tmpdir) / ".env").write_text("MINIO_ROOT_USER=user\n")
            with self.assertRaisesRegex(ValueError, "docker-compose.yml"):
                load_store_config(tmpdir)

    def test_load_rejects_symlinked_project_artifact(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            init_store(tmpdir)
            env_path = Path(tmpdir) / ".env"
            target = Path(tmpdir) / "real.env"
            env_path.replace(target)
            env_path.symlink_to(target)

            with self.assertRaisesRegex(ValueError, "symbolic link"):
                load_store_config(tmpdir)

    def test_load_rejects_missing_required_env_values(self):
        required = (
            "MINIO_ROOT_USER",
            "MINIO_ROOT_PASSWORD",
            "MINIO_PORT",
            "MINIO_CONSOLE_PORT",
            "CONTROLLER_PORT",
            "BUCKET_NAME",
            "DSIMAGING_CONTROLLER_TOKEN",
            "DSIMAGING_WEBHOOK_TOKEN",
        )
        for key in required:
            with self.subTest(key=key), tempfile.TemporaryDirectory() as tmpdir:
                init_store(tmpdir)
                env_path = Path(tmpdir) / ".env"
                lines = [
                    line for line in env_path.read_text().splitlines()
                    if not line.startswith(f"{key}=")
                ]
                env_path.write_text("\n".join(lines) + "\n")

                with self.assertRaisesRegex(ValueError, key):
                    load_store_config(tmpdir)

    def test_load_rejects_invalid_port_instead_of_defaulting(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            init_store(tmpdir)
            env_path = Path(tmpdir) / ".env"
            env_path.write_text(
                env_path.read_text().replace("MINIO_PORT=9000", "MINIO_PORT=invalid")
            )

            with self.assertRaisesRegex(ValueError, "MINIO_PORT"):
                load_store_config(tmpdir)


class FakeDoctorS3:
    def __init__(self, *, versioning="Enabled", notification=None,
                 marker_present=True):
        self.versioning = versioning
        self.notification = notification if notification is not None else {
            "QueueConfigurations": [{
                "QueueArn": "arn:minio:sqs::DSIMAGING:webhook",
                "Events": ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"],
                "Filter": {"Key": {"FilterRules": [{
                    "Name": "prefix", "Value": "datasets/",
                }]}},
            }],
        }
        self.marker_present = marker_present
        self.calls = []

    def head_bucket(self, *, Bucket):
        self.calls.append(("head_bucket", Bucket))

    def get_bucket_versioning(self, *, Bucket):
        self.calls.append(("get_bucket_versioning", Bucket))
        return {"Status": self.versioning} if self.versioning else {}

    def get_bucket_notification_configuration(self, *, Bucket):
        self.calls.append(("get_bucket_notification_configuration", Bucket))
        return self.notification

    def head_object(self, *, Bucket, Key):
        self.calls.append(("head_object", Bucket, Key))
        if not self.marker_present:
            raise KeyError(Key)
        return {"ContentLength": 0}


class StoreDoctorContractTests(unittest.TestCase):
    def _doctor(self, tmpdir, s3):
        with patch("dsimaging_admin.store.compose_ps", return_value="running"), \
                patch("dsimaging_admin.store.health", return_value={"status": "ok"}), \
                patch("dsimaging_admin.store.create_client", return_value=s3):
            return store_doctor(tmpdir)

    def test_doctor_checks_generated_s3_contract(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            init_store(tmpdir)
            s3 = FakeDoctorS3()
            result = self._doctor(tmpdir, s3)

        self.assertTrue(result["ok"])
        self.assertTrue(result["s3"]["ok"])
        self.assertEqual(
            result["s3"]["versioning"],
            {"ok": True, "status": "Enabled"},
        )
        self.assertTrue(result["s3"]["init_marker"]["ok"])
        self.assertTrue(result["s3"]["notification"]["ok"])
        self.assertEqual(s3.calls, [
            ("head_bucket", "imaging-data"),
            ("get_bucket_versioning", "imaging-data"),
            ("get_bucket_notification_configuration", "imaging-data"),
            ("head_object", "imaging-data", "datasets/.keep"),
        ])

    def test_doctor_fails_when_generated_s3_contract_is_incomplete(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            init_store(tmpdir)
            s3 = FakeDoctorS3(
                versioning="Suspended", notification={}, marker_present=False)
            result = self._doctor(tmpdir, s3)

        self.assertFalse(result["ok"])
        self.assertFalse(result["s3"]["ok"])
        self.assertFalse(result["s3"]["versioning"]["ok"])
        self.assertFalse(result["s3"]["init_marker"]["ok"])
        self.assertFalse(result["s3"]["notification"]["ok"])
        self.assertIn("versioning", result["s3"]["error"])
        self.assertIn("datasets/.keep", result["s3"]["error"])
        self.assertIn("notification", result["s3"]["error"])


if __name__ == "__main__":
    unittest.main()
