"""Exercise release preparation against a temporary local Git remote."""
import os
from pathlib import Path
import subprocess
import tempfile
import tomllib
import unittest

SCRIPT = Path(__file__).with_name("prepare-release.py").resolve()
SUBDIR = "macros" if (SCRIPT.parents[2] / "macros").is_dir() else "derive"


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        self.git("init", "-b", "main")
        self.git("config", "user.name", "Release test")
        self.git("config", "user.email", "test@example.com")
        self.git("config", "commit.gpgsign", "false")
        self.git("config", "tag.gpgsign", "false")
        (self.repo / SUBDIR / "src").mkdir(parents=True)
        (self.repo / "src").mkdir()
        (self.repo / "src/lib.rs").write_text("")
        (self.repo / SUBDIR / "src/lib.rs").write_text("")
        (self.repo / "Cargo.toml").write_text(f'''[package]
name = "release-test"
version = "0.1.0"
edition = "2021"
license = "MIT"
description = "Release test"
[workspace]
members = ["{SUBDIR}"]
[dependencies]
release-test-macro = {{ path = "{SUBDIR}", version = "0.1.0" }}
''')
        (self.repo / SUBDIR / "Cargo.toml").write_text('''[package]
name = "release-test-macro"
version = "0.1.0"
edition = "2021"
''')
        self.git("add", ".")
        self.git("commit", "-m", "Initial fixture")
        self.git("tag", "v0.0.4")
        self.git("init", "--bare", str(self.root / "remote.git"))
        self.git("remote", "add", "origin", str(self.root / "remote.git"))
        self.git("push", "origin", "main", "--tags")

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.repo, stderr=subprocess.DEVNULL, text=True).strip()

    def prepare(self, retry=""):
        return subprocess.run(
            [os.sys.executable, str(SCRIPT)], cwd=self.repo,
            env={**os.environ, "DEFAULT_BRANCH": "main", "RETRY_TAG": retry,
                 "GITHUB_OUTPUT": str(self.root / "output")},
            text=True, capture_output=True,
        )

    def test_new_release_and_retry(self):
        result = self.prepare()
        self.assertEqual(result.returncode, 0, result.stderr)
        manifest = tomllib.loads((self.repo / "Cargo.toml").read_text())
        self.assertEqual(manifest["package"]["version"], "0.0.5")
        self.assertEqual(manifest["dependencies"]["release-test-macro"]["version"], "=0.0.5")
        nested = tomllib.loads((self.repo / SUBDIR / "Cargo.toml").read_text())
        self.assertEqual(nested["package"]["version"], "0.0.5")
        self.git("tag", "v0.0.5")
        self.git("push", "origin", "main", "--tags")
        head = self.git("rev-parse", "HEAD")
        result = self.prepare("v0.0.5")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.git("rev-parse", "HEAD"), head)

    def test_missing_retry_tag_has_clear_error(self):
        result = self.prepare("v0.0.99")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Leave retry_tag empty", result.stderr)

    def test_historical_tag_version_mismatch(self):
        result = self.prepare("v0.0.4")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Tag and Cargo version do not match", result.stderr)

    def test_unpushed_head_is_rejected(self):
        self.git("commit", "--allow-empty", "-m", "Unpushed")
        result = self.prepare()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Default branch has advanced", result.stderr)


if __name__ == "__main__":
    unittest.main()
