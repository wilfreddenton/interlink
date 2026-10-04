"""Prepare a disposable dependency-only patch, preserving the published lockfile."""

import json
from pathlib import Path
import shutil
import subprocess


SOURCE = Path(__file__).resolve().parent
DESTINATION = SOURCE.parents[1] / "target" / "rustqueue-evaluation"


def main():
    metadata = json.loads(subprocess.check_output([
        "cargo", "metadata", "--manifest-path", str(SOURCE / "Cargo.toml"),
        "--offline", "--locked", "--format-version", "1",
    ], text=True))
    package = next(p for p in metadata["packages"] if p["name"] == "rustqueue")
    assert package["version"] == "0.3.0"
    upstream = Path(package["manifest_path"]).parent
    patched = DESTINATION / "upstream"
    if patched.exists():
        shutil.rmtree(patched)
    shutil.copytree(upstream, patched)
    manifest = (patched / "Cargo.toml").read_text()
    for dependency in ["reqwest", "metrics-exporter-prometheus"]:
        section = f"[dependencies.{dependency}]\n"
        assert manifest.count(section) == 1
        manifest = manifest.replace(section, section + "default-features = false\n")
    (patched / "Cargo.toml").write_text(manifest)
    for directory in ["src", "tests"]:
        target = DESTINATION / directory
        if target.exists():
            shutil.rmtree(target)
        shutil.copytree(SOURCE / directory, target)
    shutil.copyfile(SOURCE / "Cargo.lock", DESTINATION / "Cargo.lock")
    manifest = (SOURCE / "Cargo.toml").read_text()
    manifest += '\n[patch.crates-io]\nrustqueue = { path = "upstream" }\n'
    (DESTINATION / "Cargo.toml").write_text(manifest)
    # Cargo adjusts the copied lockfile for the local patch; the original stays pinned.
    subprocess.run([
        "cargo", "metadata", "--manifest-path", str(DESTINATION / "Cargo.toml"),
        "--offline", "--format-version", "1",
    ], check=True, stdout=subprocess.DEVNULL)
    print(f"Prepared dependency-only patch at {DESTINATION}")


if __name__ == "__main__":
    main()
