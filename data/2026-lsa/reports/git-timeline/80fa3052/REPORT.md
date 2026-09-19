# Sachsen-Anhalt 2026: die zehn wichtigsten Befunde

**Zehn Tweets in steigender Priorität: 1 = ergänzender Kontext, 10 = wichtigste Gesamtbewertung.** Die Reihenfolge richtet sich nach der Bedeutung für die Verlässlichkeit der veröffentlichten Ergebnisse: zunächst Einordnung und Quellenlimits, dann belegte Änderungen und abschließend der vollständige Summenabgleich. Das ist eine redaktionelle Gewichtung, kein statistischer Schweregrad.

Die Serie bündelt zusammengehörige Beobachtungen zu zehn Befunden. Alle 26 Grafiken, die politischen Repräsentationsdaten und die vollständigen Prüfbelege sind in Serie und Anhang zugänglich. Ergänzende Ergebnis- und Verteilungsgrafiken stehen im Anhang.

Archiv-Endpunkt `80fa3052044a45af29f4f0b2867957d8a3b1df35`, letzter Datencommit `eeae7f0d9e0cbf3b825b19eb14727dc64704ac7b`. Letzter Git-Abruf **07.09.2026, 04:06:37,856 MESZ**. 124 Daten-Commits, 122 datierte Abrufe: 119 ab Wahlabend, 3 frühe Nullvorlagen; 2 Einrichtungsschritte ohne Abrufzeit. Alle Zeiten sind Europe/Berlin (MESZ, UTC+2).

[Vergleich beider Analysen](EXTERNAL_COMPARISON.md) · [Methoden und Reproduktion](METHODS.md) · [Prüfurteil](VALIDATION.md) · [Tweet-Texte](tweets.txt)

## Zehn Befunde, von niedriger zu höherer Priorität

10 einzelne Entwürfe mit höchstens 280 gewichteten Zeichen. Tweet 10 hat die höchste Priorität. Alle LSA-Abrufzeiten unten sind MESZ. Bilder, Belege und Bildtexte gehören zur Berichtskopie; es wurde nichts veröffentlicht.

### Tweet 1: Frühe Parteianteile hängen von der Meldestichprobe ab

1/10 Vom ersten positiven zum letzten Git-Stand sinkt der AfD-Anteil relativ: BW −21,7 %, RLP −16,8 %, SA −26,9 %. Keine Prozentpunkte. Die frühen Stichproben unterscheiden sich; der dokumentierte RLP-Parserfehler betrifft keine gezeigte Partei.

![Relative Änderung der Parteianteile: erster → letzter Stand · Zweitstimmen / RLP: Landesstimmen · (letzter Anteil − erster Anteil) ÷ erster Anteil × 100 · keine Prozentpunkte](charts/25_drei_wahlen_relativer_anteilsdelta.png)

Belege: [cross_election_share_delta.csv](cross_election_share_delta.csv), [cross_election_endpoints.json](cross_election_endpoints.json).

### Tweet 2: Unvollständige Teilstände liefern keine belastbare Beteiligungsquote

2/10 152 Teilstände zeigen mehr Wählende als veröffentlichte Wahlberechtigte: 17 überlappende Gebiete, kein vollständiger Stand. Beispiel Balgstädt: Briefwahl trifft auf einen unvollständigen Urnen-Nenner. Der Quotient ist dann keine belastbare Beteiligungsquote.

Belege: [turnout_denominator_observations.json](turnout_denominator_observations.json), [METHODS.md](METHODS.md).

### Tweet 3: Statistische Hinweise sind keine festgestellten Wahlfehler

3/10 Die robuste MAD-Prüfung liefert 118 Merkmals-Hinweise in 106 Gebieten, davon 91 zu GRÜNE-Anteilen. Alle 24.171 prüfbaren MAD-Werte sind reproduziert. Die Vergleichsgruppen berücksichtigen keine Ortsstruktur; die Hinweise sind keine nachgewiesenen Wahlfehler.

![Die 118 statistischen Hinweise sind reproduzierbar · GFrei.News-Schwellen nachgerechnet · 106 verschiedene Gebiete · keine 118 festgestellten Fehler](charts/19_statistische_hinweise.png)

![Hohe GRÜNE-Anteile im örtlichen Vergleich · Zwölf größte robuste GRÜNE-Abstände · jeweilige Gemeinde, Wahlart und Größenklasse ergänzen den Vergleich](charts/20_hinweise_ortsstruktur.png)

Belege: [external_mad_recalculation.csv](external_mad_recalculation.csv), [screening_local_context.csv](screening_local_context.csv).

### Tweet 4: Beide Analysen stimmen bei den geprüften Daten überein

4/10 Lauras Wahlforensik und unser Archiv stimmen in 135.837 Zahlen überein. Alle 86 externen Revisionsfelder passen exakt zu Git-Übergängen. Unsere 20 Gemeinderevisionen zählen andere Einheiten; die Zahlen widersprechen sich nicht.

![Zwei Analysen: gleiche Werte, andere Zähleinheiten · Unsere Git-Historie und Lauras Wahlforensik · versionierter Datenvergleich](charts/21_vergleich_analysen.png)

Belege: [EXTERNAL_COMPARISON.md](EXTERNAL_COMPARISON.md), [external_revision_comparison.json](external_revision_comparison.json).

### Tweet 5: Entfernte Meldezähler sind ein Schemawechsel

5/10 Die vorläufige Landes-CSV entfernt Ist/Soll-Meldezähler. Das ist ein Schemawechsel, kein Rückgang auf 0/0. HTML und Gemeinde-CSV zeigen 2.661/2.661; der neue Einzelbezirk-Export enthält genau dieselben 2.661 Identitäten.

![Meldestand am Ende des Archivs · 2.661 gemeldete Bezirke; 2.661 individuelle Ergebnisse. Landes-CSV ohne Meldezähler.](charts/07_offene_meldungen.png)

Belege: [source_schema_events.json](source_schema_events.json), [final_reconciliation.json](final_reconciliation.json).

### Tweet 6: Aken erklärt den Anstieg auf 2.661 Wahlbezirke

6/10 Aken 000010 wird zwischen 00:45:41 und 00:51:10 MESZ neu sichtbar; das Soll steigt von 2.660 auf 2.661. Die damaligen +435 Zweitstimmen der Gemeinde sind nicht die finalen 1.139 dieses Bezirks. Seine frühere Stimmenzuordnung fehlt.

![Ein neuer Eintrag erhöht das Soll um eins · Aken 000010 · 00:45:41 → 00:51:10 MESZ · bereits gemeldet](charts/14_aken_neuer_wahlbezirk.png)

Belege: [AKEN_000010.md](AKEN_000010.md), [district_identity_changes.json](district_identity_changes.json).

### Tweet 7: 20 Gemeinderevisionen sind im Verlauf dokumentiert

7/10 20 Änderungen bei gleicher Meldezahl in 19 Gemeinden, davon 17 nach Vollmeldung. Fünf nach Mitternacht: Bitterfeld-Wolfen, Harzgerode, Tangermünde, Leuna und Querfurt. Änderungen sind belegt; ihre Ursachen erklärt das Archiv nicht.

![Fünf Gemeinderevisionen nach Mitternacht · Unveränderte Zahl gemeldeter Bezirke · E = Erststimmen, Z = Zweitstimmen](charts/16_revisionen_nach_mitternacht.png)

Belege: [summary.json](summary.json), [post_midnight_revisions.json](post_midnight_revisions.json), [raw_candidate_events.csv](raw_candidate_events.csv).

### Tweet 8: Bitterfeld 000028 verliert vorübergehend seinen Meldestatus

8/10 Bitterfeld-Wolfen 000028: einzige beobachtete Statusrücknahme, ab 19:35 nicht gemeldet, ab 22:11 wieder gemeldet. Der späte Einzelstand enthält 1.024 Zweitstimmen. Der Parteidiff 19:35/22:11 ist nur für die Gemeinde verfügbar, nicht für diesen Bezirk.

![Eine Meldung wird zurückgenommen · Bitterfeld-Wolfen, Stadt / 000028 · Statushistorie; Einzelstimmen zu den beiden historischen Zeitpunkten fehlen](charts/08_status_ruecknahme.png)

![Bitterfeld-Wolfen: Stimmen der gesamten Gemeinde · Abrufe 19:35 → 22:11 MESZ · 13/31 → 31/31 Wahlbezirke · Einzelbezirk 000028 nicht isolierbar](charts/13_bitterfeld_gemeindediff.png)

Belege: [BITTERFELD_WOLFEN_000028.md](BITTERFELD_WOLFEN_000028.md), [bitterfeld_party_diff.csv](bitterfeld_party_diff.csv).

### Tweet 9: Die geschlossene Aschersleben-Lücke passt exakt zu Bezirk 000965

9/10 Die Aschersleben-Lücke von 1.513 Zweitstimmen besteht in drei Abrufen 03:04–03:10 und ist um 04:06 geschlossen. Alle Parteiwerte passen zum jetzt einzeln belegten Briefwahlbezirk 000965. Der genaue Zeitpunkt des Abgleichs zwischen den Abrufen bleibt offen.

![Die Aschersleben-Lücke ist geschlossen · Gemeindesumme minus Landes-CSV · gültige Zweitstimmen · archivierte Beobachtungen](charts/15_aggregationsdifferenz.png)

Belege: [AGGREGATION_ASCHERSLEBEN.md](AGGREGATION_ASCHERSLEBEN.md), [aggregation_gap.json](aggregation_gap.json), [final_precinct_cases.json](final_precinct_cases.json).

### Tweet 10: Die abschließende Gesamtprüfung ergibt übereinstimmende Summen

10/10 Gesamtbefund: Alle 2.661 Wahlbezirke ergeben exakt die Landes-, Kreis-, Wahlkreis- und Gemeindesummen: 1.315.315 gültige Zweitstimmen. Keine finalen Rechenabweichungen. Das erklärt historische Änderungen nicht und ist keine amtliche Endfeststellung.

![Vier unabhängige Summen ergeben denselben Landeswert · Gebietsebenen einzeln summiert · 1.315.315 gültige Zweitstimmen · keine Ebenen zusammengezählt](charts/17_finaler_aggregatabgleich.png)

Belege: [final_reconciliation.json](final_reconciliation.json), [final_aggregation_checks.csv](final_aggregation_checks.csv), [VALIDATION.md](VALIDATION.md).

## Grafikanhang: Ergebnisse und ergänzende Einordnung

Diese Grafiken ergänzen die zehn Befunde. Sie sind keine weiteren Tweets. Dazu gehören die Landesergebnisse, regionale Verteilungen, AfD-Anteile beim Eintreffen der Gebiete und die politische Repräsentation.

![Auszählung im Verlauf · CSV-Meldezähler bis 03:10 · letzter Stand: 100 % laut HTML und Gemeinde-CSV](charts/01_auszaehlung.png)

![Zweitstimmen am eingefrorenen Stand · Parteien mit landesweit mehr als 5 % · 1.315.315 gültige Zweitstimmen · 100,00 % der Wahlbezirke](charts/02_parteien.png)

![Parteianteile während der Auszählung · Feste Parteiauswahl vom Berichtsstand · frühe Meldungen sind keine repräsentative Stichprobe](charts/03_parteiverlauf.png)

![Die drei kreisfreien Städte im Vergleich · Zweitstimmen in % · übrige Gemeinden nach gültigen Stimmen gewichtet · Meldestand je Zeile](charts/04_staedte.png)

![Alle 14 Kreise und kreisfreien Städte · Zweitstimmen in % · alle Summen mit Wahlbezirken abgeglichen](charts/05_kreise.png)

![Alle 41 Wahlkreise · Zweitstimmen in % · dieselbe Skala in beiden Tafeln · alle Summen mit Wahlbezirken abgeglichen](charts/06_wahlkreise.png)

![Spätere Änderungen von Summen · 11 Gemeinde-Änderungen bei gleicher Zahl gemeldeter Wahlbezirke · Parteienverschiebungen separat im Audit](charts/09_summenrevisionen.png)

Die elf hier gezeigten Änderungen von Wähler- oder gültigen Stimmensummen sind eine Teilmenge der 20 Gemeinderevisionen. Die übrigen neun verändern nur Partei-/Kandidatenwerte bei unveränderten Gesamtsummen.

![Übersicht und CSV laufen zeitweise auseinander · Differenz der gemeldeten Wahlbezirke: HTML-Übersicht minus Landes-CSV · keine Differenz von Stimmen](charts/10_quellenversatz.png)

![Urnen- und Briefwahl: unterschiedliche Parteianteile · Gültige Zweitstimmen: Urne 953.528 · Brief 361.787 · je eigener Nenner](charts/11_urne_brief.png)

![Parteien über 5 %: Urnen- und Briefwahlanteil · Gültige Zweitstimmen am Endstand · 1.315.315 insgesamt · jeder Balken = 100 % der jeweiligen Partei](charts/26_urne_brief_ueber_5.png)

![Streuung zwischen Gemeinden und Wahlkreisen · Zweitstimmen in % · jede Gebietseinheit gleich gewichtet · große Punkte zeigen die Landesanteile](charts/12_gebietsstreuung.png)

![Parteianteile in allen 2.661 Wahlbezirken · Jeder Bezirk zählt einmal · alphabetische Parteifolge · Rauten: Landesanteile](charts/18_wahlbezirke_streuung.png)

![AfD-Anteile beim Eintreffen erster Gebietsergebnisse · x: erster positiver Stand je Gebiet · y: AfD-Anteil zu genau diesem Abruf · keine nachträglich eingesetzten Endanteile](charts/22_afd_erste_ergebnisse.png)

![AfD-Anteile: Wann Gebiete erstmals vollständig melden · x: erster Vollständigkeitsbeleg je Gebiet · y: AfD-Anteil zu genau diesem Abruf · keine nachträglich eingesetzten Endanteile](charts/23_afd_vollmeldungen.png)

![Landtagswahl Sachsen-Anhalt 2026 · Politische Repräsentation · 1.224.292 Zweitstimmen für Parteien mit Sitzen · 93,08 % der gültigen Stimmen · 71,73 % der Wahlberechtigten](charts/24_politische_repraesentation.png)

## Detailanhang: alle Revisionen bei gleicher Meldezahl

| Zeit (MESZ) | Gemeinde | Meldestand | Änderungen |
|---|---|---:|---|
| 19:42:10 | Altmärkische Wische | 5/5 | TIERSCHUTZALLIANZ (Z) -1; Tierschutzpartei (Z) +1 |
| 20:45:17 | Tangerhütte, Stadt | 11/21 | CDU (Z) +1; AfD (Z) -1 |
| 21:05:09 | Genthin, Stadt | 5/14 | dieBasis (Z) -3; Tierschutzpartei (Z) +3 |
| 21:10:26 | Oberharz am Brocken, Stadt | 12/12 | CDU (E) +1; Die Linke (E) +1; GRÜNE (E) +1; gültige Erststimmen +3 |
| 22:11:13 | Allstedt, Stadt | 15/15 | AfD (E) +1; AfD (Z) +1; gültige Erststimmen +1; gültige Zweitstimmen +1; Wählende +1 |
| 22:11:13 | Bad Dürrenberg, Stadt | 10/10 | Wählende -42 |
| 22:11:13 | Aschersleben, Stadt | 23/26 | Wählende +1 |
| 22:11:13 | Annaburg, Stadt | 14/14 | SPD (E) +17; FDP (E) -17 |
| 22:33:43 | Mücheln (Geiseltal), Stadt | 12/12 | FDP (E) +4; GRÜNE (E) -4; FDP (Z) +4; GRÜNE (Z) -4 |
| 22:50:14 | Quedlinburg, Welterbestadt | 18/18 | AfD (Z) -1; Die Linke (Z) +1 |
| 22:50:14 | Südharz | 18/18 | CDU (Z) -1; FDP (Z) -1; gültige Zweitstimmen -2 |
| 23:01:15 | Mücheln (Geiseltal), Stadt | 12/12 | CDU (E) -2; AfD (E) -1; BSW (Z) -2; AfD (Z) -1; gültige Erststimmen -3; gültige Zweitstimmen -3 |
| 23:50:43 | Naumburg (Saale), Stadt | 31/31 | Wählende +4 |
| 23:50:43 | Möser | 9/9 | Wählende -38 |
| 00:00:20 | Gröningen, Stadt | 6/6 | BSW (E) +1; Die Linke (E) -1; SPD (E) +1; FDP (E) -1 |
| 00:23:44 | Bitterfeld-Wolfen, Stadt | 31/31 | CDU (E) -1; AfD (E) -2; Die Linke (E) +50; GRÜNE (E) -28; FREIE WÄHLER (E) -19 |
| 00:45:41 | Harzgerode, Stadt | 13/13 | Wählende -21 |
| 01:52:10 | Tangermünde, Stadt | 14/14 | Wählende -24 |
| 02:03:12 | Leuna, Stadt | 14/14 | Die Linke (Z) -9; GRÜNE (Z) +8; gültige Zweitstimmen -1 |
| 02:03:12 | Querfurt, Stadt | 16/16 | PdF (Z) +1; Volt (Z) -1 |

Diese Auswahl umfasst positive wie negative Parteiverschiebungen bei unveränderter Meldezahl. Sie ist kein vollständiges Verzeichnis sachlicher Korrekturen: Änderungen zusammen mit neuen Meldungen können darin fehlen. Die vollständigen Kandidaten einschließlich U/B-Teilsummen stehen in [raw_candidate_events.csv](raw_candidate_events.csv).

## Einzelfälle und Prüfgrenzen

[Aschersleben 000965: Lücke und Auflösung](AGGREGATION_ASCHERSLEBEN.md) · [Aken 000010: Identität und tatsächliche Stimmen](AKEN_000010.md) · [Bitterfeld-Wolfen 000028: historischer Diff und finaler Bezirkstand](BITTERFELD_WOLFEN_000028.md)

Die Ergebnisse sind vorläufige amtlich veröffentlichte Zahlen, keine endgültige Feststellung. Exakte Arithmetik schließt plausible, aber sachlich falsche Eingaben nicht aus. Ursachen der Rücknahme, Gemeindekorrekturen und späteren Aufnahme des Aken-Eintrags sind im geprüften Material nicht erklärt. Ausgleichende Korrekturen und Änderungen zwischen Abrufen können unsichtbar bleiben.

In einer frühen Nullvorlage fehlt die ursprüngliche CSV; ihr normalisierter Git-Export wurde geprüft. Der Zeitraum vor der ersten Statusübersicht um 19:05 bietet keine bezirksscharfe Statushistorie. Vor dem letzten Git-Abruf fehlen individuelle Wahlbezirk-Stimmen vollständig. Die externe Analyse hat andere Abrufzeitpunkte; ihr Software-Commit ist kein zusätzlicher Commit dieses Git-Archivs.

[Alle Parteien und Gebiete](all_party_results_by_area.csv) · [Finale Summenprüfungen](final_aggregation_checks.csv) · [Historische Summendifferenzen](aggregation_nonzero.csv) · [CSV-Schemawechsel](source_schema_events.json)

## Vergleich zur ursprünglichen 98,27%-Auswertung

Seit dem damaligen Stand: +46.543 gültige Zweitstimmen, einschließlich Nachmeldungen und Korrekturen. Alle 46 damals offenen Statuszeilen sind geschlossen. Der Nenner stieg durch Aken von 2.660 auf 2.661. [Alle Parteien im Vergleich](comparison.json).


## Frühe Nenner und Vergleich der drei Wahlen

152 Teilstands-Beobachtungen in 17 überlappenden Gebieten ergeben Wählende > veröffentlichte Wahlberechtigte; keine davon ist vollständig gemeldet. Beispiel Balgstädt 19:19: 93 Urnenwählende aus einem von drei Urnenbezirken mit 134 Wahlberechtigten plus 155 Briefwählende, zusammen 248. Dieser gemischte Teilstand erlaubt keine Beteiligungsquote 248/134. Zeilenbilanzen bleiben korrekt. [Alle betroffenen Zustände](turnout_denominator_observations.json).

Der Vergleich BW/RLP/SA prüft die ersten und letzten positiven archivierten Landesanteile. BW/RLP beruhen auf normalisierten Git-Exporten; ursprüngliche Nutzdaten sind dort nicht so vollständig archiviert wie für LSA. Im ersten RLP-Stand ist die normalisierte Wählerzahl inkonsistent mit den gültigen Stimmen; Ursache ist der belegte Parserfehler, der FREIE WÄHLER als Wählerzahl erfasste (später behoben in Git 80b01c01). Die Restdifferenz beträgt genau 4.652 Stimmen. Die sechs gezeigten Parteizähler und der Nenner 90.362 bleiben unverändert; es wird nicht auf die unvollständige Parteisumme umnormiert. Der spätere RLP-PREP_ZERO-Reset wird nicht als Wahl-Endergebnis interpretiert. Die Exportdateien der Endpunkte liegen unter cross_election_sources.
