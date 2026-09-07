# Sachsen-Anhalt: reproduzierbares Berichtspaket

Öffnen: data/2026-lsa/reports/git-timeline/80fa3052/REPORT.md

Das Paket enthält 10 deutsche Tweet-Entwürfe in steigender Priorität,
25 PNG-Grafiken in Serie und Anhang, CSV/JSON-Belege,
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

Validiert: 18 Regressionstests, acht ausgeführte Notebook-Zellen, 124 exakt
reproduzierte Berichts-, Daten- und Grafikdateien. Dieses Paket wurde nicht
veröffentlicht; Erfassung und Deployment werden vom Packager nicht verändert.
