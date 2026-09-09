#!/usr/bin/env python3
"""Rebuild Halle map inputs from the adjacent official ZIP archives."""
import json
from pathlib import Path
import tempfile
import zipfile
import shapefile
from pyproj import CRS, Transformer
from shapely.geometry import shape, mapping
from shapely.ops import transform

HERE = Path(__file__).resolve().parent
for kind, uuid in [
    ("urn", "f977c527-18af-a33e-1095-607752b26ed0"),
    ("postal", "f3adb8a7-6326-e10e-f75a-5e0ce46f2c2b"),
]:
    with tempfile.TemporaryDirectory() as temp:
        with zipfile.ZipFile(HERE / f"halle_{kind}.zip") as archive:
            archive.extractall(temp)
        shp = next(Path(temp).glob("*.shp"))
        reader = shapefile.Reader(str(shp))
        source_crs = CRS.from_wkt(shp.with_suffix(".prj").read_text())
        assert source_crs.to_epsg() == 2398
        transformer = Transformer.from_crs(source_crs, 25832, always_xy=True)
        features = []
        for rec in reader.shapeRecords():
            props = rec.record.as_dict()
            props.update({"booth_id": props.get("wbz", props.get("briefwbz")).zfill(6),
                          "municipality_id": "15002000", "voting_mode": "U" if kind == "urn" else "B"})
            geom = transform(transformer.transform, shape(rec.shape.__geo_interface__))
            features.append({"type": "Feature", "properties": props, "geometry": mapping(geom)})
        output = {"type": "FeatureCollection", "source": f"https://webapp.halle.de/komgis30.hal.opendata/{uuid}.html",
                  "source_download": f"https://webapp.halle.de/komgis30.hal.opendata/{uuid}_shp.zip",
                  "source_crs": "EPSG:2398", "crs": {"type": "name", "properties": {"name": "EPSG:25832"}},
                  "election_date": "2026-09-06", "attribution": "Datenquelle: Stadt Halle (Saale)",
                  "license": "CC BY 3.0 DE", "features": features}
        (HERE / f"halle_{kind}_2026_epsg25832.geojson").write_text(json.dumps(output, ensure_ascii=False, separators=(",", ":")))
