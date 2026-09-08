"""Publish macros first; verify an existing artifact before resuming a release."""
import hashlib
import json
from pathlib import Path
import subprocess
import tomllib
import urllib.error
import urllib.request


def run(*args):
    subprocess.run(args, check=True)


def publish(manifest):
    package = tomllib.loads(Path(manifest).read_text())["package"]
    name, version = package["name"], package["version"]
    # Always compile the packaged source, even when retrying a completed upload.
    run("cargo", "publish", "-p", name, "--locked", "--dry-run", "--registry", "crates-io")
    request = urllib.request.Request(
        f"https://index.crates.io/{name[:2]}/{name[2:4]}/{name}",
        headers={"User-Agent": "we-breeze-release (https://github.com/we-breeze)"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            entries = [json.loads(line) for line in response]
            existing = next((entry for entry in entries if entry["vers"] == version), None)
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise
        existing = None
    if existing is not None:
        artifact = Path("target/package") / f"{name}-{version}.crate"
        checksum = hashlib.sha256(artifact.read_bytes()).hexdigest()
        if existing.get("cksum") != checksum:
            raise SystemExit(f"{name} {version} exists with different contents; refusing to skip it")
        print(f"{name} {version} already published with matching contents")
        return
    run("cargo", "publish", "-p", name, "--locked", "--registry", "crates-io")


if __name__ == "__main__":
    publish("derive/Cargo.toml")
    publish("Cargo.toml")
