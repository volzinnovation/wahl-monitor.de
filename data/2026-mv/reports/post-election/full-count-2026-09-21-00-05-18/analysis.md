# Mecklenburg-Vorpommern 2026 · Bericht zum vollständig erfassten Stand

1.974 von 1.974 Wahlbezirken sind erfasst; 1.015.323 gültige Zweitstimmen liegen vor. AfD liegt mit 38.22 % der erfassten Zweitstimmen vorn.

Die demografische Analyse verknüpft 116 Ämter und amtsfreie Gemeinden mit dem Zensus 2022. Urnen- und Briefstimmen sind in diesen Gebieten gemeinsam enthalten.

Unter den vollständig abgedeckten Merkmalen zeigt Die Linke mit „Anteil 19–24 Jahre“ den größten absoluten gewichteten Zusammenhang (r = 0.91). Dies ist eine explorative Auswahl, keine kausale Erklärung.

Die amtliche Sitzverteilung liegt diesem Bericht noch nicht bei. Der Repräsentations-Waterfall ist deshalb ausdrücklich ein Szenario, kein Nachweis tatsächlich zugeteilter Sitze.

Zensuswerte beschreiben alle Einwohner bzw. die angegebenen Haushalts-/Personengruppen von 2022, nicht die Wahlberechtigten von 2026. Gebietsmerkmale lassen keine individuellen Wahlentscheidungen erkennen.

Altersbänder sind hier 67+ und 19–24 Jahre (veröffentlichte Zensusklassen); sie entsprechen nicht den LSA-Bändern 65+ und 18–29 aus 2025. Dichte und Bevölkerungsänderung 2024–25 werden ohne passende Datengrundlage nicht berechnet.

Unterdrückte oder fehlende Zensuswerte werden nicht durch null ersetzt. Paarweise Stichprobengrößen und Stimmenabdeckung werden für jedes Merkmal ausgewiesen.

Pearson und Spearman werden gleichgewichtet und stimmengewichtet ausgegeben; die binäre Stimmkorrelation bezeichnet das Wohngebiet als Kontext. Es werden keine p-Werte, Betrugssignale oder Individualeffekte behauptet.

Vollständige Auszählung bedeutet einen vollständig erfassten (gegebenenfalls vorläufigen) Stand, nicht ein amtlich festgestelltes Endergebnis.

Gemeinsame Amtsbriefwahl darf keiner einzelnen Gemeinde zugerechnet werden. MV wird deshalb auf Amtsebene bzw. für amtsfreie Gemeinden analysiert. Der Ausschluss von Rostock und Schwerin sowie die acht Kreisblöcke sind separat nachprüfbar.

## Politische Repräsentation

Szenario: Parteien ab 5 % oder mit aktueller Erststimmenführung; keine amtliche Sitzfeststellung. 872.394 Stimmen · 85.92 % der erfassten gültigen Zweitstimmen.

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



## Streuung · Wahlbezirke

Deskriptive Gebietsverteilung, keine Unsicherheitsintervalle.

![Streuung · Wahlbezirke](charts/03_streuung_wahlbezirk.png)

Daten: results_by_area.csv



## Briefwahl und Urnenwahl

Gesamtergebnis je Modus, gewichtet mit den gültigen Stimmen.

![Briefwahl und Urnenwahl](charts/04_brief_urne.png)

Daten: voting_modes.csv



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



## Die Linke und Anteil 19–24 Jahre

Jeder Punkt ist ein Gebiet, keine Person.

![Die Linke und Anteil 19–24 Jahre](charts/14_demografie_scatter_1.png)

Daten: demographic_areas.csv



## GRÜNE und Einwohnerzahl (log)

Jeder Punkt ist ein Gebiet, keine Person.

![GRÜNE und Einwohnerzahl (log)](charts/14_demografie_scatter_2.png)

Daten: demographic_areas.csv



## Geografische Validierung der Zählmodelle

Keine Prognose für Einzelpersonen; Überdispersion und Koeffizientenstabilität stehen in den CSVs.

![Geografische Validierung der Zählmodelle](charts/15_zaehlmodell.png)

Daten: model_validation.csv; model_predictions.csv

