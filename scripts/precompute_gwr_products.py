#!/usr/bin/env python3
"""Prepare validated GWR products outside the interactive request path."""
from __future__ import annotations
import argparse, json, sys
from datetime import datetime, timezone
from pathlib import Path

BASE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(BASE))
import app as webapp

MAPPING={"temperatura":"temperatura_media_c","precipitacao":"precipitacao_anual_mm"}

def prepare(disease: str, year: int, predictors: list[str], force: bool=False) -> dict:
    predictors=[MAPPING.get(p,p) for p in predictors]
    manifest=webapp._gwr_manifest_path(disease,year,predictors)
    existing=webapp._load_persisted_gwr_product(disease,year,predictors)
    if existing and not force:
        return existing
    panel,meta=webapp._build_runtime_gwr_panel(disease,year,predictors)
    panel_path=webapp._gwr_panel_path(disease,year,predictors)
    panel.to_csv(panel_path,index=False)
    from scripts.generate_epidemiological_gwr_maps import generate_epidemiological_gwr_maps
    result=generate_epidemiological_gwr_maps(
        panel_path,webapp.DEFAULT_PERNAMBUCO_CARTOGRAPHY,"desfecho",predictors,
        output_dir=webapp._runtime_gwr_dir(),analysis_year=year,
        save_joined_geodata=True,dpi=300,
        title_prefix=f"EpiGeoData | GWR {disease} x {' + '.join(predictors)}"
    )
    def rel(path): return path.relative_to(BASE).as_posix()
    payload={
        "schema_version":1,"prepared_at":datetime.now(timezone.utc).isoformat(),
        "disease_key":disease,"year":year,"predictors":predictors,
        "panel_file":rel(panel_path),"joined_geojson_file":rel(result.joined_data_path),
        "map_files":{k:rel(v) for k,v in result.map_paths.items()},
        "bandwidth":result.gwr_bandwidth,"records_used":result.records_used,
        "methodology":meta["method"]+"; ajuste GWR estadual validado; recortes territoriais não refazem o modelo",
        "sources":{"climate":"INMET Dados Históricos Anuais","epidemiology":"DATASUS/TABNET","territory":"IBGE Malha Municipal"}
    }
    manifest.write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding="utf-8")
    return payload

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--disease",required=True)
    p.add_argument("--year",required=True,type=int)
    p.add_argument("--predictors",nargs="+",required=True)
    p.add_argument("--force",action="store_true")
    a=p.parse_args()
    print(json.dumps(prepare(a.disease,a.year,a.predictors,a.force),ensure_ascii=False,indent=2))

if __name__=="__main__": main()
