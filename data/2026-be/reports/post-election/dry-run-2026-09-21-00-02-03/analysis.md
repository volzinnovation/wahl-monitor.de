# Berlin 2026 · Probelauf mit aktuellem Datenstand

4.071 von 4.114 Wahlbezirken sind erfasst; 1.804.830 gültige Zweitstimmen liegen vor. Die Linke liegt mit 25.72 % der erfassten Zweitstimmen vorn.

Die demografische Analyse verknüpft 12 Bezirke mit dem Zensus 2022. Urnen- und Briefstimmen sind in diesen Gebieten gemeinsam enthalten.

Unter den vollständig abgedeckten Merkmalen zeigt GRÜNE mit „Abitur/Fachhochschulreife“ den größten absoluten gewichteten Zusammenhang (r = 0.94). Dies ist eine explorative Auswahl, keine kausale Erklärung.

Die amtliche Sitzverteilung liegt diesem Probelauf noch nicht bei. Der Repräsentations-Waterfall ist deshalb ausdrücklich ein Szenario, kein Nachweis tatsächlich zugeteilter Sitze.

Die Auszählung ist offen. Fehlende Meldungen sind geografisch selektiv; alle Ergebnisse und Korrelationen dieses Probelaufs können sich ändern. Das CSV enthält zusätzlich eine Auswertung nur vollständig gemeldeter Gebiete.

Zensuswerte beschreiben alle Einwohner bzw. die angegebenen Haushalts-/Personengruppen von 2022, nicht die Wahlberechtigten von 2026. Gebietsmerkmale lassen keine individuellen Wahlentscheidungen erkennen.

Altersbänder sind hier 67+ und 19–24 Jahre (veröffentlichte Zensusklassen); sie entsprechen nicht den LSA-Bändern 65+ und 18–29 aus 2025. Dichte und Bevölkerungsänderung 2024–25 werden ohne passende Datengrundlage nicht berechnet.

Unterdrückte oder fehlende Zensuswerte werden nicht durch null ersetzt. Paarweise Stichprobengrößen und Stimmenabdeckung werden für jedes Merkmal ausgewiesen.

Pearson und Spearman werden gleichgewichtet und stimmengewichtet ausgegeben; die binäre Stimmkorrelation bezeichnet das Wohngebiet als Kontext. Es werden keine p-Werte, Betrugssignale oder Individualeffekte behauptet.

Vollständige Auszählung bedeutet einen vollständig erfassten (gegebenenfalls vorläufigen) Stand, nicht ein amtlich festgestelltes Endergebnis.

Die aktuelle normalisierte Quelle enthält Wahlkreise und Bezirke, aber keine einzelnen Wahlbezirke. Wahlbezirksstreuung bleibt deshalb aus. Bei zwölf Bezirken wird das fünfvariable LSA-Zählmodell nicht geschätzt.

Brief-/Urnenvergleich: separat datierte, amtlich veröffentlichte Prozentwerte. Absolute Stimmen und gültige Stimmen je Modus werden in dieser Quelle nicht angegeben und nicht zurückgerechnet.

## Politische Repräsentation

Szenario: Parteien ab 5 % oder mit aktueller Erststimmenführung; keine amtliche Sitzfeststellung. 1.571.849 Stimmen · 87.09 % der erfassten gültigen Zweitstimmen.

![Politische Repräsentation](charts/01_politische_repraesentation.png)

Daten: representation_waterfall.csv; representation_sources.json



## Erst- und Zweitstimmen

Unterschiedliche Stimmarten haben eigene Nenner.

![Erst- und Zweitstimmen](charts/02_parteien.png)

Daten: party_results.csv



## Streuung · Wahlkreise

Deskriptive Gebietsverteilung, keine Unsicherheitsintervalle.

![Streuung · Wahlkreise](charts/03_streuung_wahlkreis.png)

Daten: results_by_area.csv



## Briefwahl und Urnenwahl

Amtlich publizierte Prozentwerte je Modus · 20.09.2026, 23:59:22 MESZ · Parteien über 5 % in mindestens einem Modus. Absolute Stimmen/Nenner fehlen.

![Briefwahl und Urnenwahl](charts/04_brief_urne.png)

Daten: voting_modes.csv; sources/official_voting_modes.html



## Stärkste Partei nach Zweitstimmen

Kartierung der aktuellen Wahlkreisergebnisse; keine Sitzmehrheiten.

![Stärkste Partei nach Zweitstimmen](charts/05_staerkste_partei.png)

Daten: 05_staerkste_partei.csv; sources/wahlkreise.geojson



## AfD: absolute Mehrheit der Zweitstimmen

Kartierung der aktuellen Wahlkreisergebnisse; keine Sitzmehrheiten.

![AfD: absolute Mehrheit der Zweitstimmen](charts/06_afd_mehrheit.png)

Daten: 06_afd_mehrheit.csv; sources/wahlkreise.geojson



## Linke, SPD und GRÜNE: gemeinsame Zweitstimmenmehrheit

Kartierung der aktuellen Wahlkreisergebnisse; keine Sitzmehrheiten.

![Linke, SPD und GRÜNE: gemeinsame Zweitstimmenmehrheit](charts/07_linke_spd_gruene.png)

Daten: 07_linke_spd_gruene.csv; sources/wahlkreise.geojson



## Verlauf der Auszählung

Der Nenner stammt aus jedem einzelnen Abruf.

![Verlauf der Auszählung](charts/08_auszaehlung.png)

Daten: counting_timeline.csv



## Parteianteile im Verlauf

Frühe Ergebnisse sind geografisch selektiv.

![Parteianteile im Verlauf](charts/09_parteien_verlauf.png)

Daten: party_timeline.csv



## AfD-Anteil beim ersten positiven Ergebnis

MV-Meldegebiete enthalten gesonderte Briefwahlgebiete; hier keine demografischen Einheiten.

![AfD-Anteil beim ersten positiven Ergebnis](charts/10_ankunft_first_positive.png)

Daten: arrival_observations.csv



## AfD-Anteil bei erster Vollmeldung

MV-Meldegebiete enthalten gesonderte Briefwahlgebiete; hier keine demografischen Einheiten.

![AfD-Anteil bei erster Vollmeldung](charts/10_ankunft_first_complete.png)

Daten: arrival_observations.csv



## Demografische Zusammenhänge

Vollständiges Merkmalsraster; unterschiedliche Abdeckung steht im Coverage-CSV.

![Demografische Zusammenhänge](charts/11_demografie.png)

Daten: demographic_correlations.csv; demographic_coverage.csv



## Was verändert die Stimmengewichtung?

Gewichte ändern die Fragestellung; sie liefern keine Individualdaten.

![Was verändert die Stimmengewichtung?](charts/12_stimmengewichtung.png)

Daten: demographic_strongest.csv



## Geografische Sensitivität

Punkt: alle Gebiete; Linie: Spannweite nach geografischem Ausschluss.

![Geografische Sensitivität](charts/13_geografische_sensitivitaet.png)

Daten: demographic_leave_one_block_out.csv



## GRÜNE und Abitur/Fachhochschulreife

Jeder Punkt ist ein Gebiet, keine Person.

![GRÜNE und Abitur/Fachhochschulreife](charts/14_demografie_scatter_1.png)

Daten: demographic_areas.csv



## Die Linke und Anteil 67 Jahre und älter

Jeder Punkt ist ein Gebiet, keine Person.

![Die Linke und Anteil 67 Jahre und älter](charts/14_demografie_scatter_2.png)

Daten: demographic_areas.csv

