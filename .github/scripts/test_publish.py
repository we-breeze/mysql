"""Verify retry behavior without contacting or publishing to a registry."""
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import urllib.error

spec = importlib.util.spec_from_file_location("publisher", Path(__file__).with_name("publish-packages.py"))
publisher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(publisher)


class PublishTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        old = Path.cwd()
        os.chdir(self.temp.name)
        self.addCleanup(os.chdir, old)
        Path("Cargo.toml").write_text('[package]\nname = "fixture-package"\nversion = "0.0.5"\n')
        Path("target/package").mkdir(parents=True)
        Path("target/package/fixture-package-0.0.5.crate").write_bytes(b"fixture")

    def response(self, checksum):
        return io.BytesIO((json.dumps({"vers": "0.0.5", "cksum": checksum}) + "\n").encode())

    def test_matching_upload_is_skipped(self):
        checksum = hashlib.sha256(b"fixture").hexdigest()
        with patch.object(publisher, "run") as run, patch.object(publisher.urllib.request, "urlopen", return_value=self.response(checksum)):
            publisher.publish("Cargo.toml")
        self.assertEqual(run.call_count, 1)
        self.assertIn("--dry-run", run.call_args.args)

    def test_different_upload_is_rejected(self):
        with patch.object(publisher, "run") as run, patch.object(publisher.urllib.request, "urlopen", return_value=self.response("different")):
            with self.assertRaisesRegex(SystemExit, "different contents"):
                publisher.publish("Cargo.toml")
        self.assertEqual(run.call_count, 1)

    def test_absent_package_is_published(self):
        error = urllib.error.HTTPError("https://example.com", 404, "Missing", {}, None)
        with patch.object(publisher, "run") as run, patch.object(publisher.urllib.request, "urlopen", side_effect=error):
            publisher.publish("Cargo.toml")
        self.assertEqual(run.call_count, 2)
        self.assertNotIn("--dry-run", run.call_args.args)

    def test_registry_failure_does_not_publish(self):
        error = urllib.error.HTTPError("https://example.com", 503, "Unavailable", {}, None)
        with patch.object(publisher, "run") as run, patch.object(publisher.urllib.request, "urlopen", side_effect=error):
            with self.assertRaises(urllib.error.HTTPError):
                publisher.publish("Cargo.toml")
        self.assertEqual(run.call_count, 1)


if __name__ == "__main__":
    unittest.main()
