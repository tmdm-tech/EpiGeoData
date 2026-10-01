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

def persist_climate(year: int, force: bool=False) -> dict:
    """Prepare one disease-neutral municipality×year climate dimension."""
    climate_dir=BASE/"data"/"climaticas"; climate_dir.mkdir(parents=True,exist_ok=True)
    climate_path=climate_dir/f"painel_climatico_pe_{year}.csv"
    climate_manifest=climate_dir/f"manifest_climatico_pe_{year}.json"
    if climate_path.exists() and climate_manifest.exists() and not force:
        return json.loads(climate_manifest.read_text(encoding="utf-8"))
    climate,meta=webapp._historical_climate_municipal_panel(year)
    climate.to_csv(climate_path,index=False)
    payload={"schema_version":2,"year":year,"prepared_at":datetime.now(timezone.utc).isoformat(),
             "source":"INMET Dados Históricos Anuais","territory":"IBGE municípios de Pernambuco",
             "grain":"municipio×ano","independent_of_disease":True,**meta}
    climate_manifest.write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding="utf-8")
    return payload


def prepare(disease: str, year: int, predictors: list[str], force: bool=False) -> dict:
    """Prepare a disease-specific GWR only after the shared climate dimension exists."""
    disease=webapp._resolve_disease_key(disease) or disease
    predictors=[MAPPING.get(p,p) for p in predictors]
    persist_climate(year,force=force)
    manifest=webapp._gwr_manifest_path(disease,year,predictors)
    existing=webapp._load_persisted_gwr_product(disease,year,predictors)
    if existing and not force: return existing
    panel,meta=webapp._build_runtime_gwr_panel(disease,year,predictors)
    panel_path=webapp._gwr_panel_path(disease,year,predictors); panel.to_csv(panel_path,index=False)
    from scripts.generate_choropleth_brazil import load_pernambuco_municipalities
    validated_cartography=webapp._runtime_gwr_dir()/"municipios_pe_ibge_validated.geojson"
    if not validated_cartography.exists():
        load_pernambuco_municipalities().to_crs("EPSG:4674").to_file(validated_cartography,driver="GeoJSON")
    from scripts.generate_epidemiological_gwr_maps import generate_epidemiological_gwr_maps
    result=generate_epidemiological_gwr_maps(
        panel_path,validated_cartography,"desfecho",predictors,
        output_dir=webapp._runtime_gwr_dir(),analysis_year=year,
        save_joined_geodata=True,dpi=300,
        title_prefix=f"EpiGeoData | GWR {disease} x {' + '.join(predictors)}",
        render_maps=True)
    def stored(path):
        p=Path(path).resolve()
        try:return p.relative_to(BASE).as_posix()
        except ValueError:return str(p)
    payload={"schema_version":2,"prepared_at":datetime.now(timezone.utc).isoformat(),
             "disease_key":disease,"year":year,"predictors":predictors,
             "panel_file":stored(panel_path),"joined_geojson_file":stored(result.joined_data_path),
             "map_files":{k:stored(v) for k,v in result.map_paths.items()},
             "bandwidth":result.gwr_bandwidth,"records_used":result.records_used,
             "methodology":meta["method"]+"; ajuste GWR estadual validado; recortes territoriais não refazem o modelo",
             "sources":{"climate":"INMET Dados Históricos Anuais","epidemiology":"DATASUS/TABNET","territory":"IBGE Malha Municipal"}}
    manifest.write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding="utf-8")
    return payload


def prepare_all(years: list[int] | None=None, force: bool=False, fit_gwr: bool=True) -> dict:
    """Discover all disease/year intersections; no canonical disease or year."""
    catalog=webapp._epidemiology_temporal_catalog()
    all_epi_years=sorted({y for item in catalog.values() for y in item["years"]})
    requested=sorted(set(years or all_epi_years))
    current=datetime.now(timezone.utc).year
    # INMET automatic historical archive is supported from 2000 through current year.
    requested=[y for y in requested if 2000<=y<=current]
    climate_results={}; failures={}
    for year in requested:
        try: climate_results[year]=persist_climate(year,force=force)
        except Exception as exc: failures[f"climate:{year}"]=f"{type(exc).__name__}: {exc}"
    products=[]
    if fit_gwr:
        for disease,item in catalog.items():
            for year in sorted(set(item["years"]) & set(climate_results)):
                try:
                    products.append(prepare(disease,year,["temperatura","precipitacao"],force=force))
                except Exception as exc:
                    failures[f"gwr:{disease}:{year}"]=f"{type(exc).__name__}: {exc}"
    return {"schema_version":2,"climate_years":sorted(climate_results),
            "gwr_products":[{"disease_key":p["disease_key"],"year":p["year"],"records_used":p["records_used"]} for p in products],
            "failures":failures,"diseases":catalog}


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--disease")
    p.add_argument("--year",type=int)
    p.add_argument("--years",nargs="*",type=int)
    p.add_argument("--predictors",nargs="+",default=["temperatura","precipitacao"])
    p.add_argument("--all",action="store_true",help="Discover all diseases/years automatically")
    p.add_argument("--climate-only",action="store_true")
    p.add_argument("--force",action="store_true")
    a=p.parse_args()
    if a.all:
        result=prepare_all(a.years,force=a.force,fit_gwr=not a.climate_only)
    elif a.year and a.climate_only:
        result=persist_climate(a.year,force=a.force)
    elif a.disease and a.year:
        result=prepare(a.disease,a.year,a.predictors,a.force)
    else:
        p.error("use --all, or --year with --climate-only, or --disease + --year")
    print(json.dumps(result,ensure_ascii=False,indent=2))

if __name__=="__main__": main()
