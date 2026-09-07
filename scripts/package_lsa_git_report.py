#!/usr/bin/env python3
"""Package the frozen, validated LSA report without collecting or publishing data."""
import hashlib
import json
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / 'data/2026-lsa/reports/git-timeline'
REPORT = BASE / '80fa3052'


def digest(data):
    return hashlib.sha256(data).hexdigest()


def main():
    validation = json.loads((REPORT / 'verification.json').read_text())
    assert validation['structural_checks'] == 'passed'
    assert validation['visual_reviewed'] is True
    environment = json.loads((REPORT / 'environment.json').read_text())
    scripts = [ROOT / 'scripts' / name for name in environment['scripts']]
    for path in scripts:
        assert digest(path.read_bytes()) == environment['scripts'][path.name], path
    notebook = ROOT / 'analysis/lsa_git_audit.ipynb'
    execution = json.loads((REPORT / 'notebook_execution.json').read_text())
    assert execution['executed_code_cells'] == 8
    assert digest(notebook.read_bytes()) == execution['notebook_sha256']
    readme = f'''# Sachsen-Anhalt: reproduzierbares Berichtspaket

Öffnen: data/2026-lsa/reports/git-timeline/80fa3052/REPORT.md

Das Paket enthält {validation['tweet_count']} deutsche Tweet-Entwürfe in steigender Priorität,
{validation['chart_count']} PNG-Grafiken in Serie und Anhang, CSV/JSON-Belege,
die eingefrorenen Vergleichsdaten von GFrei.News, Kontextquellen, die sechs
Start-/Endstände für BW/RLP/SA, Python-Skripte und das ausgeführte Audit-Notebook.
Die ursprünglichen Git-Objekte sind nicht Bestandteil dieses ZIPs.

Für eine vollständige Neuberechnung dieses Paket in einen separaten vollständigen
Checkout von https://github.com/volzinnovation/wahl-monitor.de entpacken. Der
Checkout muss die Historie bis 80fa3052044a45af29f4f0b2867957d8a3b1df35 enthalten;
ein flacher Checkout genügt nicht. Ein Branchwechsel ist nicht erforderlich.
METHODS.md im Berichtsordner enthält die Befehle. Die Skripte lesen den festen
Git-Endpunkt und die mitgelieferten Eingaben, ohne Wahldaten live abzurufen.

Verwendete Umgebung: Python 3.10.5, Matplotlib 3.10.8, NumPy 2.2.6.
Andere Versionen können identische Zahlen mit abweichenden PNG-Bytes erzeugen.
Das Notebook kann mit python3 scripts/run_lsa_audit_notebook.py ohne Jupyter
ausgeführt werden. Die Erzeugung verändert nur lokale abgeleitete Dateien.
Um nach einer Änderung ein neues Paket zu erzeugen, zuerst Analyse, Diagramme,
Notebook und Verifikation erneut ausführen, dann den Packager starten:
python3 scripts/package_lsa_git_report.py

SHA256SUMS im ZIP prüft alle übrigen enthaltenen Dateien. Im Berichtsordner
prüft eine zweite SHA256SUMS dessen Dateien. Die neben dem ZIP gespeicherte
.sha256-Datei prüft das gesamte Paket. Die Prüfungen belegen Dateiidentität,
keine Echtheit amtlicher Wahlergebnisse.

Grenzen: Nur der letzte LSA-Abruf enthält individuelle Stimmen aller Wahlbezirke.
Historische Änderungen einzelner Bezirke lassen sich damit nicht vollständig
rekonstruieren. Statistische Hinweise sind kein Beweis für Wahlmanipulation.
Der erste RLP-Export hat einen dokumentierten FREIE-WÄHLER-Parserfehler; die
gezeigten sechs Parteien und der gültige Stimmennenner bleiben unverändert.
Die unterschiedlichen frühen Stichproben sind im Vergleichsdiagramm angegeben.

Validiert: 18 Regressionstests, acht ausgeführte Notebook-Zellen, {len(validation['reproduction_files_compared'])} exakt
reproduzierte Berichts-, Daten- und Grafikdateien. Dieses Paket wurde nicht
veröffentlicht; Erfassung und Deployment werden vom Packager nicht verändert.
'''
    (REPORT / 'PACKAGE_README.md').write_text(readme)
    report_files = sorted(p for p in REPORT.rglob('*') if p.is_file() and p.name != 'SHA256SUMS')
    manifest = ''.join(f'{digest(p.read_bytes())}  {p.relative_to(REPORT).as_posix()}\n' for p in report_files)
    (REPORT / 'SHA256SUMS').write_text(manifest)
    paths = report_files + [REPORT / 'SHA256SUMS', notebook, Path(__file__).resolve()] + scripts
    paths += [BASE / '034045f3' / name for name in ['summary.json', 'latest_areas.json']]
    entries = {p.relative_to(ROOT).as_posix(): p.read_bytes() for p in paths}
    entries['README.md'] = readme.encode()
    entries['SHA256SUMS'] = ''.join(f'{digest(data)}  {name}\n' for name, data in sorted(entries.items())).encode()
    target = BASE / 'lsa-full-history-report-80fa3052.zip'
    with zipfile.ZipFile(target, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for name, data in sorted(entries.items()):
            info = zipfile.ZipInfo(name, date_time=(2026, 9, 7, 12, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.create_system = 3
            info.external_attr = 0o100644 << 16
            archive.writestr(info, data, compresslevel=9)
    with zipfile.ZipFile(target) as archive:
        assert archive.testzip() is None
        for line in archive.read('SHA256SUMS').decode().splitlines():
            expected, name = line.split('  ', 1)
            assert digest(archive.read(name)) == expected, name
    checksum = digest(target.read_bytes())
    target.with_suffix('.zip.sha256').write_text(f'{checksum}  {target.name}\n')
    print(json.dumps({'path': str(target), 'files': len(entries), 'bytes': target.stat().st_size,
                      'sha256': checksum, 'archive_integrity': 'passed'}, indent=2))


if __name__ == '__main__':
    main()
