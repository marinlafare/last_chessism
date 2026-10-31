"""No real credential files, databases, Docker commands or cloud calls."""
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

from sqlalchemy.engine import make_url

FILE = Path(__file__).resolve().parents[1] / "stockfish-batch-worker/cloud_job/run_host.py"
SPEC = importlib.util.spec_from_file_location("cloud_host_launcher", FILE)
launcher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(launcher)


class HostLauncherTests(unittest.TestCase):
    def test_maps_only_local_compose_database_and_preserves_credentials(self):
        mapped = make_url(launcher.host_database_url("postgresql+asyncpg://test:p%40ss%2Fword@db:5432/example"))
        self.assertEqual((mapped.host, mapped.port, mapped.database), ("127.0.0.1", 5433, "example"))
        self.assertEqual(mapped.password, "p@ss/word")

    def test_rejects_remote_targets_and_invalid_config_without_echoing_secrets(self):
        for value in (None, "secret-password", "sqlite:///file", "postgresql://u:secret-password@remote/db"):
            with self.subTest(value=value), self.assertRaises(ValueError) as raised:
                launcher.host_database_url(value)
            self.assertNotIn("secret-password", str(raised.exception))
        with self.assertRaises(ValueError):
            launcher.host_database_url("postgresql://u:p@db/app", host="remote")

    def test_explicit_sdk_path_is_resolved(self):
        with patch.object(launcher.shutil, "which", return_value="/opt/google/bin/gcloud"):
            self.assertEqual(launcher.gcloud_executable("/opt/google/bin/gcloud"), "/opt/google/bin/gcloud")


if __name__ == "__main__":
    unittest.main()
