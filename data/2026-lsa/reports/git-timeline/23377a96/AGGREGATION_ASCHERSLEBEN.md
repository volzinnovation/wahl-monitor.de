# Aschersleben: Die Gemeindesumme liegt über den höheren Aggregaten

**Ab 03:04:09 MESZ enthält die Gemeinde-CSV 1.513 gültige Zweitstimmen mehr als die Landes-CSV.** Die Differenz bleibt bis zum letzten Archivlauf 03:10:11 MESZ bestehen. Alle Partei- und Summendifferenzen stimmen exakt mit dem gleichzeitig neu erfassten Zuwachs der Gemeinde Aschersleben überein.

Die Gemeinde steigt von 25/26 auf 26/26 gemeldete Bezirke. Im HTML wechselt Aschersleben / 000965 auf gemeldet; zusätzlich wechselt Petersberg / 000012 seinen Status. Eine ursprüngliche Einzelbezirk-Stimmentabelle liegt weiterhin nicht vor. Der rechnerisch passende Gemeindezuwachs ist deshalb ausdrücklich kein unabhängig belegtes Einzelbezirksergebnis.

## Dieselbe Differenz wird mehrfach sichtbar

3 archivierte Stände zeigen jeweils 27 abweichende Felder sowohl Gemeinde → Land als auch Gemeinde → Salzlandkreis: zusammen 162 nicht-null Vergleiche. Das sind Wiederholungen derselben Aggregationslücke, keine 162 unabhängigen Ereignisse.

| Größe | Amtliche Landes-CSV | Summe Gemeinde-CSV | Differenz |
|---|---:|---:|---:|
| Gemeldete Bezirke | 2.660 | 2.661 | +1 |
| Wählende | 1.326.694 | 1.328.211 | +1.517 |
| Gültige Erststimmen | 1.311.225 | 1.312.734 | +1.509 |
| Gültige Zweitstimmen | 1.313.802 | 1.315.315 | +1.513 |

Der Wahlkreis Aschersleben bleibt im höheren CSV-Aggregat bei 54/55, der Salzlandkreis bei 193/194, das Land bei 2.660/2.661. Die Gemeinde-CSV und das HTML weisen am letzten Stand alle Bezirke als gemeldet aus. Das ist mit einem nicht fortgeschriebenen höheren Aggregat vereinbar; den tatsächlichen Publikationsablauf oder Grund belegen die Dateien nicht.

## Parteivergleich

| Stimmen | Partei | Landes-CSV | Gemeindesumme | Δ |
|---|---|---:|---:|---:|
| Erststimmen | CDU | 314.120 | 314.535 | +415 |
| Erststimmen | AfD | 581.599 | 581.945 | +346 |
| Erststimmen | Die Linke | 157.758 | 157.905 | +147 |
| Erststimmen | SPD | 102.663 | 102.881 | +218 |
| Erststimmen | FDP | 41.067 | 41.113 | +46 |
| Erststimmen | GRÜNE | 45.240 | 45.310 | +70 |
| Erststimmen | FREIE WÄHLER | 14.464 | 14.464 | +0 |
| Erststimmen | Tierschutzpartei | 1.129 | 1.129 | +0 |
| Erststimmen | Gartenpartei | 2.937 | 2.937 | +0 |
| Erststimmen | Die PARTEI | 777 | 777 | +0 |
| Erststimmen | TIERSCHUTZALLIANZ | 999 | 999 | +0 |
| Erststimmen | HEIMAT | 325 | 325 | +0 |
| Erststimmen | BSW | 41.539 | 41.782 | +243 |
| Erststimmen | PdF | 2.656 | 2.680 | +24 |
| Erststimmen | Volt | 768 | 768 | +0 |
| Erststimmen | Anderer Kreiswahlvorschlag | 3.184 | 3.184 | +0 |
| Zweitstimmen | CDU | 226.221 | 226.622 | +401 |
| Zweitstimmen | AfD | 575.664 | 576.037 | +373 |
| Zweitstimmen | Die Linke | 112.395 | 112.541 | +146 |
| Zweitstimmen | SPD | 122.092 | 122.303 | +211 |
| Zweitstimmen | FDP | 33.934 | 33.969 | +35 |
| Zweitstimmen | GRÜNE | 117.376 | 117.498 | +122 |
| Zweitstimmen | FREIE WÄHLER | 15.406 | 15.430 | +24 |
| Zweitstimmen | dieBasis | 1.896 | 1.899 | +3 |
| Zweitstimmen | Tierschutzpartei | 14.338 | 14.356 | +18 |
| Zweitstimmen | Gartenpartei | 7.475 | 7.482 | +7 |
| Zweitstimmen | Die PARTEI | 3.485 | 3.487 | +2 |
| Zweitstimmen | TIERSCHUTZALLIANZ | 7.272 | 7.283 | +11 |
| Zweitstimmen | BSW | 69.143 | 69.291 | +148 |
| Zweitstimmen | PdF | 2.771 | 2.778 | +7 |
| Zweitstimmen | Volt | 4.334 | 4.339 | +5 |

Die landesweiten Parteianteile im Hauptbericht verwenden unverändert die amtliche Landes-CSV als Nenner und Quelle. Die Gemeindesumme ist eine gesondert ausgewiesene eigene Aggregation, kein ersetztes amtliches Landesergebnis.

[Vollständige berechnete Lücke](aggregation_gap.json) · [Parteitabelle CSV](land_vs_municipality_parties.csv) · [Alle nicht-null Aggregationsvergleiche](aggregation_nonzero.csv) · [Gesamtbericht](REPORT.md)

[Unveränderliche amtliche Quelldateien](https://github.com/volzinnovation/wahl-monitor.de/tree/23377a96d918a4b77f71dbabbf9c385f78334126/data/2026-lsa/latest/official_sources).
