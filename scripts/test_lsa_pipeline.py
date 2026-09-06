"""Offline election-night rehearsals, using the official pre-election CSVs."""
from __future__ import annotations

import copy
import csv
import io
import json
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import lsa_source
import poll_election_core as core
import version_lsa_results as version
import run_local_poll_loop as loop
from test_lsa_statla_csv import WAHLBEZIRK_HEADER, wahlbezirk_row

REPO = Path(__file__).resolve().parents[1]
FIXTURES = REPO / "scripts/fixtures/lsa"
BASE = "https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt26/"


def encode_rows(rows, *, reverse=False):
    fields = list(rows[0])
    if reverse:
        fields.reverse()
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fields, delimiter=";")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode("utf-8-sig")


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        shutil.copytree(REPO / "config", self.root / "config")
        shutil.copytree(REPO / "data/2026-lsa/metadata", self.root / "data/2026-lsa/metadata")
        # Never seed from live latest: tests must still pass after Wahlbezirk
        # results arrive and after the election's data evolves.
        (self.root / "data/2026-lsa/reports").mkdir()
        self.root_patch = patch.object(core, "ROOT", self.root)
        self.root_patch.start()
        self.addCleanup(self.restore_core)
        core.set_active_election(election_key="2026-lsa")
        core.ensure_directories()
        self.config = core.load_config()
        self.files = {
            BASE + "downloads/land.csv": (FIXTURES / "land.csv").read_bytes(),
            BASE + "downloads/gemeinden.csv": (FIXTURES / "gemeinden.csv").read_bytes(),
        }
        self.get_patch = patch.object(core, "statla_http_get", side_effect=self.http)
        self.http_mock = self.get_patch.start()
        self.addCleanup(self.get_patch.stop)

    def restore_core(self):
        self.root_patch.stop()
        core.set_active_election(election_key="2026-bw")

    def http(self, url, *_args, **_kwargs):
        if url == self.config.statla_downloads_url:
            content = ('<html>' + ''.join(f'<a href="{u}">CSV</a>' for u in self.files) + '</html>').encode()
        else:
            content = self.files[url]
        if isinstance(content, core.HttpResult):
            return content
        return core.HttpResult(url=url, status_code=200, content=content, error_message=None)

    def poll(self):
        with patch.object(sys, "argv", ["poll_election.py", "--election-key", "2026-lsa", "--force-run", "--skip-kommone", "--quiet"]):
            core.main()

    def latest_bytes(self):
        return {str(p.relative_to(core.LATEST_DIR)): p.read_bytes() for p in core.LATEST_DIR.rglob("*") if p.is_file()}

    def test_full_capture_provenance_and_replay(self):
        self.poll()
        metadata = version.verify(self.root)
        self.assertEqual(metadata["statla_mode"], "LIVE_CSV_DOWNLOAD")
        self.assertEqual(self.http_mock.call_count, 3)  # one page, each CSV once
        self.assertEqual(len(core.load_latest_statla_exports()["snapshots"]), 274)
        manifest = json.loads((core.LATEST_DIR / "official_sources/manifest.json").read_text())
        for source in manifest["fetches"]:
            if source["url"] in self.files:
                self.assertEqual((core.LATEST_DIR / "official_sources" / source["filename"]).read_bytes(), self.files[source["url"]])

    def test_different_column_order_and_bom(self):
        url = BASE + "downloads/gemeinden.csv"
        self.files[url] = encode_rows(lsa_source.read_rows(self.files[url]), reverse=True)
        self.poll()
        version.verify(self.root)

    def test_source_outage_preserves_latest_and_diagnostics(self):
        self.poll()
        previous = self.latest_bytes()
        url = BASE + "downloads/gemeinden.csv"
        self.files[url] = core.HttpResult(url=url, status_code=503, content=b"Unavailable", error_message="HTTP 503")
        with self.assertRaisesRegex(ValueError, "source unavailable"):
            self.poll()
        self.assertEqual(previous, self.latest_bytes())
        self.assertTrue(any(b"Unavailable" == p.read_bytes() for p in core.RAW_STATLA_DIR.rglob("*.csv")))

    def test_partial_transfer_even_with_http_200_is_rejected(self):
        url = BASE + "downloads/gemeinden.csv"
        self.files[url] = core.HttpResult(url=url, status_code=200, content=self.files[url], error_message="curl: transfer closed")
        with self.assertRaisesRegex(ValueError, "source unavailable"):
            self.poll()

    def test_missing_file_truncation_and_html_are_rejected(self):
        url = BASE + "downloads/gemeinden.csv"
        original = self.files[url]
        for body in (b"<html>Maintenance</html>", original.splitlines()[0] + b"\n", b"x;y\n1\n"):
            with self.subTest(body=body[:30]):
                self.files[url] = body
                with self.assertRaises(ValueError):
                    self.poll()
        del self.files[url]
        with self.assertRaisesRegex(ValueError, "coverage"):
            self.poll()

    def test_duplicate_areas_and_bad_votes_rejected(self):
        url = BASE + "downloads/land.csv"
        original = lsa_source.read_rows(self.files[url])
        variants = [original + [original[2]], copy.deepcopy(original), copy.deepcopy(original)]
        variants[1][2]["F01.CDU"] = "7"
        variants[2][2]["F01.CDU"] = "broken"
        for rows in variants:
            with self.subTest():
                self.files[url] = encode_rows(rows)
                with self.assertRaises(ValueError):
                    self.poll()

    def test_realistic_live_update_and_downward_correction(self):
        url = BASE + "downloads/land.csv"
        rows = lsa_source.read_rows(self.files[url])
        for votes in (100, 90):
            row = rows[2]
            row.update({"B.Wähler": str(votes), "D.Gültige.Erststimmen": str(votes),
                        "F.Gültige.Zweitstimmen": str(votes), "D01.CDU": str(votes),
                        "F01.CDU": str(votes), "Soll.Wahlbezirke": "10", "Ist.Wahlbezirke": "1"})
            self.files[url] = encode_rows(rows)
            self.poll()
            version.verify(self.root)
            land = next(r for r in core.load_latest_statla_exports()["snapshots"] if r["gebietsart"] == "LAND")
            self.assertEqual(land["valid_votes_zweit"], votes)

    def test_booth_arrival_then_disappearance(self):
        self.poll()
        url = BASE + "downloads/wahlbezirke.csv"
        self.files[url] = (WAHLBEZIRK_HEADER + "\n" + wahlbezirk_row("U", "40", "40")).encode()
        self.poll()
        self.assertEqual(version.verify(self.root)["wahlbezirk_rows"], 1)
        previous = self.latest_bytes()
        del self.files[url]
        with self.assertRaisesRegex(ValueError, "previously collected"):
            self.poll()
        self.assertEqual(previous, self.latest_bytes())

    def test_corrupt_source_bytes_block_versioning(self):
        self.poll()
        path = next((core.LATEST_DIR / "official_sources").glob("*.csv"))
        path.write_bytes(b"tampered")
        with self.assertRaisesRegex(ValueError, "checksum"):
            version.verify(self.root)

    def test_normalized_export_must_match_source(self):
        self.poll()
        path = core.LATEST_DIR / "statla_snapshots.csv"
        path.write_text(path.read_text().replace("Sachsen-Anhalt", "Incorrect name"))
        with self.assertRaisesRegex(ValueError, "source bytes"):
            version.verify(self.root)

    def init_git(self):
        version.git(self.root, "init", "-b", "archive-test")
        version.git(self.root, "add", ".")
        version.git(self.root, *version.IDENTITY, "commit", "-m", "Initial fixture")

    def test_git_version_is_scoped_and_repeat_is_noop(self):
        self.init_git()
        self.poll()
        (self.root / "unrelated.txt").write_text("Keep me")
        self.assertTrue(version.commit_results(self.root))
        self.assertFalse(version.commit_results(self.root))
        self.assertIn("?? unrelated.txt", version.git(self.root, "status", "--short"))
        changed = version.git(self.root, "diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD").splitlines()
        self.assertTrue(all(p.startswith("data/2026-lsa/") for p in changed))

    def test_staged_unrelated_changes_block_commit(self):
        self.init_git()
        self.poll()
        (self.root / "unrelated.txt").write_text("Keep me")
        version.git(self.root, "add", "unrelated.txt")
        with self.assertRaisesRegex(ValueError, "unrelated"):
            version.commit_results(self.root)

    def test_push_recovers_from_concurrent_unrelated_commit(self):
        self.init_git()
        remote = self.root / "remote.git"
        version.git(self.root, "init", "--bare", str(remote))
        version.git(self.root, "remote", "add", "origin", str(remote))
        version.push_results(self.root, "archive-test", retry_delay=0)
        other = self.root / "other"
        version.git(self.root, "clone", "--branch", "archive-test", str(remote), str(other))
        (other / "parallel.txt").write_text("Concurrent change")
        version.git(other, "add", "parallel.txt")
        version.git(other, *version.IDENTITY, "commit", "-m", "Parallel update")
        version.git(other, "push", "origin", "archive-test")
        url = BASE + "downloads/land.csv"
        rows = lsa_source.read_rows(self.files[url])
        wk = next(row for row in rows if row["Satzart"] == "WKR" and not row["Wahllokal"])
        wk.update({"Soll.Wahlbezirke": "10", "Ist.Wahlbezirke": "1"})
        self.files[url] = encode_rows(rows)  # also changes the tracked status SVG
        self.poll()
        version.commit_results(self.root)
        version.push_results(self.root, "archive-test", retry_delay=0)
        self.assertEqual((self.root / "parallel.txt").read_text(), "Concurrent change")
        self.assertEqual(version.git(self.root, "rev-parse", "HEAD"), version.git(remote, "rev-parse", "archive-test"))
        version.verify(self.root)

    def test_local_loop_continues_after_timeout_and_versions_success(self):
        argv = ["loop", "--election-key", "2026-lsa", "--iterations", "2",
                "--continue-on-error", "--version-lsa"]
        with patch.object(sys, "argv", argv), patch.object(loop, "sleep_to_next_tick") as sleep, patch.object(
            loop.subprocess, "run", side_effect=[subprocess.TimeoutExpired("poll", 180),
                                                subprocess.CompletedProcess([], 0),
                                                subprocess.CompletedProcess([], 0)]
        ) as run:
            self.assertEqual(loop.main(), 1)  # partial failures remain observable
            self.assertEqual(run.call_count, 3)
            self.assertIn("--commit", run.call_args.args[0])
            sleep.assert_called_once()  # no sleep after the final iteration

    def test_local_loop_rejects_invalid_intervals(self):
        with patch.object(sys, "argv", ["loop", "--interval-seconds", "0"]):
            with self.assertRaises(SystemExit):
                loop.parse_args()

    def test_workflow_window_boundaries_and_manual_recovery(self):
        workflow = (REPO / ".github/workflows/archive-lsa.yml").read_text()
        body = workflow.split("python - <<'PY'\n", 1)[1].split("          PY", 1)[0]
        code = compile(textwrap.dedent(body), "workflow-window", "exec")
        for date, event, expected in [
            ("2026-09-06T15:59:59", "schedule", False),
            ("2026-09-06T16:00:00", "schedule", True),
            ("2026-09-07T04:30:00", "schedule", True),
            ("2026-09-08T09:59:59", "schedule", True),
            ("2026-09-08T10:00:00", "schedule", False),
            ("2027-09-06T16:00:00", "schedule", False),
            ("2026-09-09T12:00:00", "workflow_dispatch", True),
        ]:
            with self.subTest(date=date, event=event):
                output = self.root / "github-output"
                output.write_text("")
                with patch("datetime.datetime", wraps=datetime) as clock, patch.dict(
                    "os.environ", {"EVENT_NAME": event, "GITHUB_OUTPUT": str(output)}
                ):
                    clock.now.return_value = datetime.fromisoformat(date).replace(tzinfo=timezone.utc)
                    exec(code, {})
                self.assertEqual(output.read_text(), f"run={str(expected).lower()}\n")


if __name__ == "__main__":
    unittest.main()
