"""Checks preventing LSA deployment from replacing frozen election pages."""
import hashlib
import io
import json
import tarfile
import tempfile
import unittest
from pathlib import Path

import build_lsa_pages as pages


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
