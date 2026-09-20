# Auszählung Landtagswahl Mecklenburg-Vorpommern 2026 — Zeitraffer

[MP4](auszaehlung-landtagswahl-mecklenburg-vorpommern.mp4) · [MPEG-2](auszaehlung-landtagswahl-mecklenburg-vorpommern.mpg) · [Daten](snapshots.csv)

75 archivierte Abrufe vom **2026-09-20T19:12:21.930864+02:00** bis **2026-09-20T23:51:30.273609+02:00**.
1920 × 1080, 25 Bilder/s, 80 Sekunden, ohne Ton.
Letzter Stand: **1,014,559 gültige Zweitstimmen**;
**1973 von 1974 Wahlbezirken**. Zwischenergebnis, kein amtliches Endergebnis.

## Methode und Quellen

- Gleiches Layout wie Sachsen-Anhalt: alphabetische Reihenfolge, feste 0–60%-Skala.
- Feste Auswahl: AfD, Die Linke, GRÜNE, SPD; mehr als 5 % im letzten archivierten Landesstand.
- Nenner: alle gültigen Zweitstimmen im jeweiligen Landesstand, einschließlich nicht gezeigter Parteien.
- Zeitstempel: Abschluss des gespeicherten Abrufs, nicht Zeitpunkt der Stimmabgabe oder Auszählung.
- 16748.3 Sekunden Archivzeit proportional auf 75 Sekunden komprimiert,
  plus 2 Sekunden am Anfang und 3 am Ende. Echte, unveränderte Beobachtungen ohne Interpolation;
  unveränderte Stände und Korrekturen bleiben enthalten. Nullstände und Vorlagen sind ausgeschlossen.
- Quelle: versionierte normalisierte amtliche Landesdaten, Git-Endpunkt `60845e75aa1425199bd28557b9ad5050b2675e13`.
  Jede Beobachtung enthält Git-Verweis, Originalquellen-URLs und SHA-256 der drei Eingabedateien.
  Dies ist eine Auswertung des verfügbaren Git-Archivs, keine Behauptung lückenloser amtlicher Meldungen.
- `snapshots.json` enthält sämtliche Parteistimmen. `manifest.json` dokumentiert Ausschlüsse und Prüfungen.
  Alle Parteisummen wurden mit dem Nenner abgeglichen. Beide Filme wurden vollständig dekodiert;
  Bildzahl, Auflösung und Codec wurden mit ffprobe geprüft.

## Reproduce

```sh
python3 scripts/render_state_results_animation.py --election 2026-mv --ref 60845e75aa1425199bd28557b9ad5050b2675e13 --timeline-seconds 75 --fps 25 --output /tmp/2026-mv-animation
```

Requires Python, Matplotlib, ffmpeg and ffprobe. No fetch, poll, or deployment is performed.
