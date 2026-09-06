"""Checks preventing LSA deployment from replacing frozen election pages."""
import hashlib
import io
import json
import subprocess
import tarfile
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import build_lsa_pages as pages
import generate_static_detail_pages as generator


class CurrentOverviewTests(unittest.TestCase):
    def setUp(self):
        self.snapshot = {"row_key": "land", "reported_precincts": 1, "total_precincts": 10,
                         "voters_total": 101, "valid_votes_zweit": 100, "valid_votes_erst": 0}
        self.current = [{"row_key": "land", "vote_type": "Zweitstimmen", "party_name": "CDU", "votes": 60},
                        {"row_key": "land", "vote_type": "Zweitstimmen", "party_name": "BSW", "votes": 40}]
        self.reference = {"land_area": {"valid_second_votes": 1000}, "party_rows": [
            {"area_level": "LAND", "vote_type": "Zweitstimmen", "party_name": "CDU", "votes": 300}]}

    def test_current_votes_and_percentage_point_change_use_separate_denominators(self):
        result = generator.render_lsa_current_results_panel(self.snapshot, self.current, self.reference)
        self.assertIn("<td>60</td><td>60,00 %</td><td>30,00 %</td><td>+30,00 Pp.</td>", result)
        self.assertIn("Die Gebietsabdeckung unterscheidet sich", result)
        self.assertNotIn("Sitzverteilung 2021", result)
        self.assertNotIn("Rheinland-Pfalz", result)

    def test_new_party_does_not_get_an_invented_2021_result(self):
        result = generator.render_lsa_current_results_panel(self.snapshot, self.current, self.reference)
        self.assertIn("<td>40</td><td>40,00 %</td><td>—</td><td>—</td>", result)

    def test_zero_votes_and_zero_change_are_visible(self):
        self.current[0]["votes"] = 0
        self.reference["party_rows"][0]["votes"] = 0
        result = generator.render_lsa_current_results_panel(self.snapshot, self.current, self.reference)
        self.assertIn("<td>0</td><td>0,00 %</td><td>0,00 %</td><td>+0,00 Pp.</td>", result)

    def test_empty_results_do_not_show_false_negative_swings(self):
        result = generator.render_lsa_current_results_panel({}, [], self.reference)
        self.assertIn("Landesergebnis 2026", result)
        self.assertIn("Noch keine gültigen Stimmen", result)
        self.assertNotIn("−30,00 Pp.", result)

    def test_overview_coverage_leads_when_csv_is_behind(self):
        result = generator.render_lsa_current_results_panel(
            self.snapshot,
            self.current,
            self.reference,
            {"reported_precincts": 219, "total_precincts": 2660},
        )
        self.assertIn("219 / 2.660", result)
        self.assertIn("CSV-Ergebnisstand (1 / 10 Wahlbezirke)", result)

    def test_vote_share_history_uses_fixed_zero_to_sixty_percent_axis(self):
        history = [
            {
                "timestamp_local": datetime(2026, 9, 6, 18, 0, tzinfo=timezone.utc),
                "label": "18:00",
                "reported_precincts": 1,
                "total_precincts": 10,
                "shares": {"AfD": 10.0, "CDU": 20.0, "GRÜNE": 5.0},
            },
            {
                "timestamp_local": datetime(2026, 9, 6, 18, 15, tzinfo=timezone.utc),
                "label": "18:15",
                "reported_precincts": 2,
                "total_precincts": 10,
                "shares": {"AfD": 12.0, "CDU": 19.0, "GRÜNE": 6.0},
            },
        ]
        with mock.patch.object(generator, "load_git_vote_share_history", return_value=history):
            result = generator.render_vote_share_history_panel(object())
        for tick in (0, 12, 24, 36, 48, 60):
            self.assertIn(f">{tick}%</text>", result)
        self.assertIn(">18:00</text>", result)
        self.assertIn(">18:15</text>", result)
        self.assertNotIn("rotate(90", result)

    def test_vote_share_history_reads_land_party_rows_by_shared_row_key(self):
        snapshots = (
            "row_key,gebietsart,valid_votes_zweit,reported_precincts,total_precincts\n"
            "lsa:LAND:15,LAND,100,10,20\n"
        )
        party_rows = (
            "row_key,vote_type,party_name,votes\n"
            "lsa:LAND:15,Zweitstimmen,AfD,50\n"
            "lsa:LAND:15,Zweitstimmen,CDU,20\n"
            "lsa:LAND:15,Zweitstimmen,GRÜNE,5\n"
        )

        def fake_git(args, **kwargs):
            if args[1] == "log":
                return subprocess.CompletedProcess(args, 0, "abc123\t2026-09-06T18:00:00+00:00\n", "")
            target = args[2]
            if target.endswith("run_metadata.json"):
                output = '{"generated_at_utc":"2026-09-06T18:00:00+00:00"}'
            elif target.endswith("statla_snapshots.csv"):
                output = snapshots
            else:
                output = party_rows
            return subprocess.CompletedProcess(args, 0, output, "")

        with mock.patch.object(generator.subprocess, "run", side_effect=fake_git):
            history = generator.load_git_vote_share_history(SimpleNamespace(timezone="Europe/Berlin"))
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["shares"], {"AfD": 50.0, "CDU": 20.0, "GRÜNE": 5.0})


class PreservationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.baseline = self.root / "baseline"
        self.baseline.mkdir()
        self.output = self.root / "output"

    def archive(self, files):
        with tarfile.open(self.baseline / "bw-rlp.tar.gz", "w:gz") as archive:
            for name, data in files.items():
                member = tarfile.TarInfo(name)
                member.size = len(data)
                archive.addfile(member, io.BytesIO(data))
        manifest = {"archive_sha256": pages.digest(self.baseline / "bw-rlp.tar.gz"),
                    "files": {name: {"sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}
                              for name, data in files.items()}}
        (self.baseline / "manifest.json").write_text(json.dumps(manifest))
        return manifest

    def test_restores_exact_published_bytes(self):
        files = {"2026-bw/index.html": b"BW\r\n", "2026-rlp/data.csv": b"RLP\x00"}
        manifest = self.archive(files)
        pages.restore_baseline(self.output, self.baseline)
        pages.verify_frozen_pages(self.output, manifest)
        for name, data in files.items():
            self.assertEqual((self.output / name).read_bytes(), data)

    def test_detects_changed_missing_or_added_frozen_files(self):
        for mutation in ("changed", "missing", "added"):
            with self.subTest(mutation=mutation):
                manifest = self.archive({"2026-bw/index.html": b"original"})
                pages.restore_baseline(self.output, self.baseline)
                target = self.output / "2026-bw/index.html"
                if mutation == "changed":
                    target.write_bytes(b"changed")
                elif mutation == "missing":
                    target.unlink()
                else:
                    (target.parent / "extra.html").write_bytes(b"extra")
                with self.assertRaisesRegex(ValueError, "changed"):
                    pages.verify_frozen_pages(self.output, manifest)

    def test_refuses_paths_outside_frozen_elections(self):
        self.archive({"../escape.html": b"bad"})
        with self.assertRaisesRegex(ValueError, "Unexpected"):
            pages.restore_baseline(self.output, self.baseline)
        self.assertFalse((self.root / "escape.html").exists())

    def test_refuses_corrupt_archive(self):
        self.archive({"2026-bw/index.html": b"original"})
        with (self.baseline / "bw-rlp.tar.gz").open("ab") as handle:
            handle.write(b"corruption")
        with self.assertRaisesRegex(ValueError, "checksum"):
            pages.restore_baseline(self.output, self.baseline)


if __name__ == "__main__":
    unittest.main()
