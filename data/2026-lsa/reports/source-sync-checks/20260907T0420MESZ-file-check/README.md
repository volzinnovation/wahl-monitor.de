# Vorläufige Ergebnisse: Dateiprüfung am 07.09.2026, 04:20 MESZ

Die CSV-Adressen sind unverändert. Alle vier angebotenen CSV-Dateien sind
bytegleich mit dem gespeicherten Abruf von 04:03 MESZ. Die Downloads tragen
jetzt die Bezeichnung „Vorläufige Ergebnisse“.

Die Datei für Land, Kreise und Wahlkreise enthält weiterhin die Stimmen,
aber keine Spalten `Uhrzeit`, `Soll.Wahlbezirke` und `Ist.Wahlbezirke`.
Die aktuelle Webseite interpretiert die fehlenden Zähler irrtümlich als 0/0
und zeigt deswegen „keine Daten“ sowie ein falsches HTML−CSV-Delta von 2.661.

Der Pages-Workflow kann mit Commit `c07240b3` jetzt einen archivierten Commit
gezielt auswählen. Noch kein Rollback wurde ausgelöst. Als verifiziertes
Rollback-Ziel steht der vorherige erfolgreiche Build aus `23377a96` bereit.
