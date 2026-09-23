"""Mandatory safety gate for legacy spatial modelling entrypoints.

Validation is deliberately independent of Flask: CLI and direct Python callers must
not bypass the restrictions imposed on the HTTP endpoints. No data are invented.
"""
from __future__ import annotations

from pathlib import Path

from stage3_validation import DataValidationError, read_panel_csv, validate_municipal_panel
import pandas as pd
import geopandas as gpd
import re


class ModelNotAuthorized(DataValidationError):
    """A model must not run before its data and scientific protocol are approved."""


def require_authorized_gwr(
    tabular_data_path: str | Path,
    municipalities_path: str | Path,
    dependent_var: str,
    independent_vars: list[str],
) -> None:
    """Authorize only a provenance-complete, single-period municipal analytical panel."""
    path, geom_path = Path(tabular_data_path), Path(municipalities_path)
    if not path.is_file(): raise ModelNotAuthorized(f"Epidemiological source unavailable: {path}")
    if not geom_path.is_file(): raise ModelNotAuthorized("Municipal geometry source unavailable")
    if not dependent_var or not independent_vars: raise ModelNotAuthorized("Dependent and independent variables are required")
    if path.suffix.lower() != ".csv": raise ModelNotAuthorized("Validated GWR input must be CSV")
    try:
        frame=pd.read_csv(path)
        required={"municipio_ibge","ano",dependent_var,*independent_vars,
                  "epidemiology_provenance","climate_provenance","territory_provenance"}
        missing=required-set(frame.columns)
        if missing: raise ModelNotAuthorized(f"Missing validated panel fields: {sorted(missing)}")
        codes=frame["municipio_ibge"].astype(str).str.replace(r"\.0$","",regex=True)
        if not codes.str.fullmatch(r"\d{7}").all(): raise ModelNotAuthorized("Invalid IBGE municipality codes")
        if frame["ano"].nunique()!=1: raise ModelNotAuthorized("GWR requires one explicit analytical period per model")
        if frame[["municipio_ibge","ano"]].duplicated().any(): raise ModelNotAuthorized("Duplicate municipality-period rows")
        for col in [dependent_var,*independent_vars]:
            numeric=pd.to_numeric(frame[col],errors="coerce")
            if numeric.isna().any() or not numeric.map(lambda x: abs(float(x)) != float("inf")).all():
                raise ModelNotAuthorized(f"Missing/non-finite model variable: {col}")
            if numeric.nunique()<2: raise ModelNotAuthorized(f"No variance in model variable: {col}")
        for prov in ("epidemiology_provenance","climate_provenance","territory_provenance"):
            if frame[prov].isna().any(): raise ModelNotAuthorized(f"Missing provenance: {prov}")
        gdf=gpd.read_file(geom_path)
        code_col=next((x for x in ("code_muni","municipio_ibge","CD_MUN") if x in gdf.columns),None)
        if not code_col: raise ModelNotAuthorized("Official geometry lacks municipality code")
        gcodes=gdf[code_col].astype(str).str.replace(r"\.0$","",regex=True)
        panel_codes=set(codes); geometry_codes=set(gcodes)
        if not panel_codes.issubset(geometry_codes):
            raise ModelNotAuthorized("Panel contains municipality codes absent from official geometry")
        if len(panel_codes) < max(30, len(independent_vars) + 10):
            raise ModelNotAuthorized("Insufficient complete municipalities for a stable GWR cross-section")
        if gdf.crs is None: raise ModelNotAuthorized("Geometry CRS is unknown")
    except ModelNotAuthorized: raise
    except Exception as exc: raise ModelNotAuthorized(f"Panel validation failed: {exc}") from exc
    return None
