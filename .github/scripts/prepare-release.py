"""Prepare a local release commit; the workflow pushes only after all checks pass."""
import os
from pathlib import Path
import re
import subprocess
import tomllib


def git(*args):
    return subprocess.check_output(["git", *args], text=True).strip()


MANIFESTS = ["Cargo.toml", "derive/Cargo.toml"]


def package(path="Cargo.toml"):
    return tomllib.loads(Path(path).read_text())["package"]


def main():
    branch = os.environ["DEFAULT_BRANCH"]
    retry = os.environ.get("RETRY_TAG", "")
    git("fetch", "origin", branch, "--tags")
    if retry:
        if not re.fullmatch(r"v0\.0\.(0|[1-9][0-9]*)", retry):
            raise SystemExit("retry_tag must be v0.0.x")
        exists = subprocess.run(
            ["git", "show-ref", "--verify", "--quiet", f"refs/tags/{retry}"]
        )
        if exists.returncode:
            raise SystemExit(
                f"Tag {retry} does not exist. Leave retry_tag empty to create a new release."
            )
        git("merge-base", "--is-ancestor", f"refs/tags/{retry}", f"origin/{branch}")
        git("checkout", "--detach", f"refs/tags/{retry}")
        if any(package(path)["version"] != retry[1:] for path in MANIFESTS):
            raise SystemExit("Tag and Cargo version do not match")
        tag = retry
    else:
        if git("rev-parse", "HEAD") != git("rev-parse", f"origin/{branch}"):
            raise SystemExit("Default branch has advanced; start a new Publish run")
        patches = [int(t[5:]) for t in git("tag", "--list", "v0.0.*").splitlines()
                   if re.fullmatch(r"v0\.0\.(0|[1-9][0-9]*)", t)]
        tag = f"v0.0.{max(patches, default=0) + 1}"

    manifest = package()
    if not (manifest.get("license") or manifest.get("license-file")):
        raise SystemExit("Set the project license or license-file in Cargo.toml before publishing")
    if not manifest.get("description"):
        raise SystemExit("Set package.description before publishing")

    if not retry:
        macro_name = package(MANIFESTS[1])["name"]
        for manifest_path in MANIFESTS:
            path = Path(manifest_path)
            content, count = re.subn(r'(?m)^version = "[^\"]+"$',
                                    f'version = "{tag[1:]}"', path.read_text(), count=1)
            if count != 1:
                raise SystemExit(f"Could not update version in {path}")
            if manifest_path == "Cargo.toml":
                lines = content.splitlines(keepends=True)
                matches = 0
                for i, line in enumerate(lines):
                    if macro_name in line and "path =" in line:
                        lines[i], n = re.subn(r'version = "[^\"]+"', f'version = "={tag[1:]}"', line)
                        matches += n
                if matches != 1:
                    raise SystemExit("Could not update macro dependency version")
                content = "".join(lines)
            path.write_text(content)
        subprocess.run(["cargo", "check", "--workspace"], check=True)
        git("config", "user.name", "github-actions[bot]")
        git("config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com")
        git("add", *MANIFESTS, "Cargo.lock")
        git("commit", "-m", f"chore: release {tag}")
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write(f"tag={tag}\n")
    print(f"Prepared {tag}")


if __name__ == "__main__":
    main()
