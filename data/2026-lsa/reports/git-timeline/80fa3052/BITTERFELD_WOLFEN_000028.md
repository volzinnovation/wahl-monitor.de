# Bitterfeld-Wolfen, Wahlbezirk 000028: Was lässt sich nach Parteien vergleichen?

**Ein Parteistimmen-Diff für Wahlbezirk 000028 ist mit diesen archivierten Quellen nicht bestimmbar.** Die Übersicht enthält nur den Meldestatus. Die Stimmenexporte der beiden historischen Abrufe reichen hier bis zur Gemeinde. Die folgenden Zahlen sind ausdrücklich die gesamte Gemeinde Bitterfeld-Wolfen.

Verglichen werden die Abrufe **2026-09-06T19:35:33.866455+02:00** (`54022b534f45331a7c3b6d920ff9a1ddd2a52510`) und **2026-09-06T22:11:13.043213+02:00** (`ba1fc517c286fae14ef9d1ae8b573aab69dc9d84`).

| Größe | Abruf 19:35 | Abruf 22:11 | Änderung |
|---|---:|---:|---:|
| Meldestatus Bezirk 000028 | nicht gemeldet (0) | gemeldet (1) | 0 → 1 |
| Gemeldete Bezirke der Gemeinde | 13/31 | 31/31 | +18 |
| Wählende | 7.657 | 21.641 | +13.984 |
| Gültige Erststimmen | 7.549 | 21.389 | +13.840 |
| Gültige Zweitstimmen | 7.566 | 21.421 | +13.855 |

Amtliche Zeitstempel der Gemeinde-CSV-Zeile: **06.09.2026 19:27:45** bzw. **06.09.2026 21:48:00**. Diese unterscheiden sich von den Abrufzeiten.

![Gemeinde-Vergleich, keine Einzelbezirksergebnisse](charts/13_bitterfeld_gemeindediff.png)


## Zweitstimmen: gesamte Gemeinde

| Partei | 19:35 | 22:11 | Δ Stimmen |
|---|---:|---:|---:|
| AfD | 3.939 | 10.517 | +6.578 |
| CDU | 1.260 | 3.589 | +2.329 |
| Die Linke | 572 | 1.780 | +1.208 |
| SPD | 548 | 1.721 | +1.173 |
| GRÜNE | 395 | 1.310 | +915 |
| BSW | 357 | 1.031 | +674 |
| FDP | 146 | 454 | +308 |
| FREIE WÄHLER | 102 | 346 | +244 |
| Tierschutzpartei | 99 | 283 | +184 |
| TIERSCHUTZALLIANZ | 54 | 119 | +65 |
| Gartenpartei | 35 | 98 | +63 |
| Volt | 30 | 76 | +46 |
| Die PARTEI | 19 | 58 | +39 |
| PdF | 7 | 27 | +20 |
| dieBasis | 3 | 12 | +9 |

## Erststimmen: gesamte Gemeinde

| Partei | 19:35 | 22:11 | Δ Stimmen |
|---|---:|---:|---:|
| AfD | 4.136 | 11.015 | +6.879 |
| CDU | 1.471 | 4.315 | +2.844 |
| Die Linke | 714 | 2.227 | +1.513 |
| SPD | 540 | 1.601 | +1.061 |
| FREIE WÄHLER | 296 | 945 | +649 |
| GRÜNE | 209 | 732 | +523 |
| FDP | 183 | 554 | +371 |

## Weshalb auch der letzte Schritt 000028 nicht isoliert

Zwischen **22:06** (`e91d1f5d`) und **22:11** melden in Bitterfeld-Wolfen gleichzeitig **5 Bezirke** neu: 000004, 000008, 000009, 000022, 000028. Die Gemeinde steigt dabei von 26/31 auf 31/31 und von 17.858 auf 21.421 gültige Zweitstimmen.

Diesen gemeinsamen Zuwachs ausschließlich Bezirk 000028 zuzuschreiben wäre falsch. Für einen Vorher/Nachher-Stimmenvergleich dieses Bezirks wären dessen ursprüngliche und korrigierte Ergebnismeldungen oder eine amtliche Korrekturdokumentation nötig. Auch ein später veröffentlichter Endstand allein liefert den alten Einzelbezirkstand nicht nach.

## Quellen und Reproduktion

[Alle Parteidifferenzen für Gemeinde, Wahlkreis und Kreis als CSV](bitterfeld_party_diff.csv) · [Strukturierter Fall mit Originalwerten](bitterfeld_case.json) · [Gesamtbericht](REPORT.md)

Die Fallauswertung wird von `scripts/render_lsa_tweet_report.py` aus dem geprüften Git-Audit reproduziert. [Quelle 19:35](https://github.com/volzinnovation/wahl-monitor.de/tree/54022b534f45331a7c3b6d920ff9a1ddd2a52510/data/2026-lsa/latest/official_sources) · [Quelle 22:11](https://github.com/volzinnovation/wahl-monitor.de/tree/ba1fc517c286fae14ef9d1ae8b573aab69dc9d84/data/2026-lsa/latest/official_sources)


## Neu: tatsächlicher Einzelbezirkstand um 04:06 MESZ

Briefwahlbezirk 000028: **1.028 Wählende, 1.021 gültige Erststimmen, 1.024 gültige Zweitstimmen.** Dieser eine späte Einzelbezirkstand liefert keinen Parteidiff zwischen 19:35 und 22:11 nach.

| Partei | Erststimmen | Zweitstimmen |
|---|---:|---:|
| AfD | 282 | 258 |
| BSW | — | 46 |
| CDU | 325 | 290 |
| Die Linke | 175 | 129 |
| Die PARTEI | — | 0 |
| FDP | 39 | 27 |
| FREIE WÄHLER | 41 | 15 |
| GRÜNE | 52 | 87 |
| Gartenpartei | — | 8 |
| PdF | — | 0 |
| SPD | 107 | 137 |
| TIERSCHUTZALLIANZ | — | 4 |
| Tierschutzpartei | — | 17 |
| Volt | — | 6 |
| dieBasis | — | 0 |

Spätere Gemeindeänderung um 00:23 MESZ: CDU (E) -1; AfD (E) -2; Die Linke (E) +50; GRÜNE (E) -28; FREIE WÄHLER (E) -19. Keine Zuordnung zum einzelnen Bezirk möglich.
