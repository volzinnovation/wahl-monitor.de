#!/usr/bin/env python3
"""Build LSA pages while preserving the published BW/RLP files verbatim."""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import tarfile
from pathlib import Path

import poll_election_core as core
import version_lsa_results

FROZEN_KEYS = ("2026-bw", "2026-rlp")
BASELINE = core.ROOT / "data/published-site"


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def restore_baseline(output: Path, baseline: Path = BASELINE) -> dict:
    manifest = json.loads((baseline / "manifest.json").read_text())
    archive = baseline / "bw-rlp.tar.gz"
    if digest(archive) != manifest["archive_sha256"]:
        raise ValueError("Published-site archive checksum mismatch")
    expected = manifest["files"]
    seen = set()
    with tarfile.open(archive, "r:gz") as source:
        for member in source:
            path = Path(member.name)
            if (not member.isfile() or member.name not in expected or member.name in seen
                    or path.is_absolute() or ".." in path.parts or path.parts[0] not in FROZEN_KEYS):
                raise ValueError(f"Unexpected published-site archive member: {member.name}")
            content = source.extractfile(member).read()
            if hashlib.sha256(content).hexdigest() != expected[member.name]["sha256"]:
                raise ValueError(f"Published file checksum mismatch: {member.name}")
            target = output / member.name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            seen.add(member.name)
    if seen != set(expected):
        raise ValueError("Published-site archive is incomplete")
    return manifest


def verify_frozen_pages(output: Path, manifest: dict) -> None:
    actual = {str(path.relative_to(output)): digest(path)
              for key in FROZEN_KEYS for path in (output / key).rglob("*") if path.is_file()}
    expected = {name: item["sha256"] for name, item in manifest["files"].items()}
    if actual != expected:
        raise ValueError("BW/RLP published pages changed during the LSA build")


def result_fingerprints() -> dict:
    names = subprocess.check_output(
        ["git", "ls-files", "-z", "--", "data/2026-bw", "data/2026-rlp", "config/2026-bw.json", "config/2026-rlp.json"],
        cwd=core.ROOT,
    ).decode().split("\0")
    return {name: digest(core.ROOT / name) for name in names if name}


def validate_lsa(output: Path) -> None:
    root = output / "2026-lsa"
    payload = json.loads((root / "search.json").read_text())
    if payload["electionKey"] != "2026-lsa" or payload["entryCount"] != len(payload["entries"]):
        raise ValueError("Invalid LSA search index")
    for entry in payload["entries"]:
        if not (root / entry["href"]).is_file():
            raise ValueError(f"Broken LSA search link: {entry['href']}")
    for directory, count in (("landkreis", 14), ("wahlkreis", 41)):
        if len(list((root / directory).glob("*.html"))) != count:
            raise ValueError(f"Missing LSA {directory} pages")
    if not 218 <= len(list((root / "municipality").glob("*.html"))) <= 227:
        raise ValueError("Missing LSA municipality pages")
    metadata = json.loads((core.ROOT / "data/2026-lsa/latest/run_metadata.json").read_text())
    if len(list((root / "booth").glob("*.html"))) != metadata.get("wahlbezirk_rows", 0):
        raise ValueError("LSA Wahlbezirk result/page count mismatch")
    scenario = json.loads((root / "scenario-data.json").read_text())
    if scenario.get("electionKey") != "2026-lsa" or not (root / "scenario.html").is_file():
        raise ValueError("Missing LSA scenario page")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=core.ROOT / "site")
    args = parser.parse_args()
    metadata = version_lsa_results.verify(core.ROOT)
    before = result_fingerprints()
    manifest = restore_baseline(args.output_root)
    subprocess.run([sys.executable, str(core.ROOT / "scripts/generate_static_detail_pages.py"),
                    "--election-key", "2026-lsa", "--output-root", str(args.output_root / "2026-lsa")],
                   cwd=core.ROOT, check=True)
    verify_frozen_pages(args.output_root, manifest)
    if before != result_fingerprints():
        raise ValueError("BW/RLP result files changed during the LSA build")
    validate_lsa(args.output_root)
    proof = {"source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=core.ROOT, text=True).strip(),
             "result_generated_at_utc": metadata["generated_at_utc"],
             "preserved_files": len(manifest["files"]),
             "preserved_archive_sha256": manifest["archive_sha256"]}
    (args.output_root / "2026-lsa/build_metadata.json").write_text(json.dumps(proof, indent=2) + "\n")
    print(json.dumps(proof, indent=2))


if __name__ == "__main__":
    main()
