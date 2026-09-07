# Vergleich mit Lauras Wahlforensik (GFrei.News)

Der Vergleich verwendet einen eingefrorenen öffentlichen Export der [verlinkten Analyse](https://gfrei.news/wp-content/uploads/wahlbeobachtung/). Deren Software-Commit und Uhrzeit gehören zu ihrem eigenen System. Sie werden nicht als zusätzliche Git-Wahlbeobachtung gezählt.

Externe Version `09303583f6526bc22f686fbf9e3e77562d0e7d7cebb51b531ddb3324ececbb7a`, erzeugt `2026-09-07T08:24:22.991634+00:00`. Datenstände und JSON-Teile wurden lokal mit SHA-256 archiviert: [Manifest](external/manifest.json), [Katalog](external/current.json). Die drei gemeinsamen Ergebnis-CSV-Hashes sind identisch; ebenso die weiteren gemeinsam gespeicherten HTML-Dateien. Ein späterer externer Prüfzeitpunkt bedeutet daher keine neueren Stimmen.

| Gegenstand | Vergleichsergebnis |
|---|---|
| Originalwerte | 3.483 Zeilen einschließlich U/B und Gesamtsummen, 135.837 Zahlen, 0 Differenzen |
| Arithmetik | Beide Analysen melden keine Bilanz-/Parteisummenfehler; unser Historienaudit umfasst zusätzlich alle früheren Git-Stände |
| Revisionen | Alle 86 extern markierten Felder passen mit Vorher-/Nachher-Werten exakt zu Git-Übergängen. 33 Kombinationen aus Gebiets-/Wahlartzeile und Git-Übergang; keine 86 unabhängigen Vorfälle |
| Unser Revisionsmaß | 20 Übergänge von Gemeinde-Gesamtsummen bei gleicher Meldezahl, in 19 Gemeinden. Positive und negative Parteiverschiebungen zählen mit |
| Robuste MAD-Prüfung | Alle 24.171 prüfbaren Werte einschließlich Median, MAD, z, Gruppengröße und Farbklasse unabhängig reproduziert; 18 unzureichende Vergleiche gesondert |
| Statistische Hinweise | 88 orange + 30 rote Merkmalswerte in 106 verschiedenen Gebieten. 91 Hinweise betreffen GRÜNE |
| Endziffern | Alle fünf Stichproben und zehn Ziffernhäufigkeiten pro Partei reproduziert. Externe q-Werte jeweils 1; Simulationen hier nicht erneut ausgeführt |
| Multivariate / räumliche Verfahren | Katalog und drei multivariate Ergebnislisten archiviert; Modelle, historische Vergleichsanteile und Geometrieprüfungen nicht unabhängig neu gerechnet. Keine Übernahme als neue Fehlerfeststellung |

Die robusten Gruppen trennen Gebietsebene, Wahlart und Zehnerpotenz der Wählerzahl. Sie berücksichtigen keine Stadt-/Landlage, Sozialstruktur oder Kandidaten. Die 91 GRÜNE-Hinweise sind daher auffällige Abstände in diesem Modell, keine 91 Wahlfehler. Die ergänzte Grafik zeigt für die zwölf größten Abstände auch den Median vergleichbarer Bezirke derselben Gemeinde.

Die Endziffernprüfung berücksichtigt hier disjunkte Wahlbezirke: gültige Zweitstimmen >400, Parteistimmen mindestens 100 und geschätzte binomiale Standardabweichung mindestens 10; mindestens 100 geeignete Bezirke pro Partei. Die aktuelle Berechnungsbeschreibung nennt diesen Bezirksvorrang; die allgemeine Feldbeschreibung spricht noch von Gemeinden. Der Ergebnisexport benennt WBZ und ist für die überprüfte Population maßgeblich.

Inhaltlich besteht bei den verglichenen Werten kein Widerspruch. Unser Beitrag ergänzt die vollständige Git-Zeitfolge, die 19:35–22:11-Statusrücknahme, das genaue Auftauchen von Aken und die belegte Auflösung der Aschersleben-Lücke. Die andere Analyse ergänzt ein breites statistisches Screening. Diese Verfahren sind abhängig und dürfen nicht als mehrere unabhängige Beweise für einen Fehler addiert werden.

[Alle Zahlenprüfungen](external_comparison.json) · [Exakte Revisionszuordnung](external_revision_comparison.json) · [Alle robusten Neuberechnungen](external_mad_recalculation.csv) · [Örtlicher Kontext](screening_local_context.csv) · [Gesamtbericht](REPORT.md)
