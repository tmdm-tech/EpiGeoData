#!/usr/bin/env python3
"""Strict GWR entrypoint. Never silently change observations or publish unvalidated maps.

The scientific authorization gate remains closed until official source provenance,
GERES mapping and climate alignment are independently homologated.
"""
from __future__ import annotations

import argparse
import math
import re
from dataclasses import dataclass
from pathlib import Path

from stage3_pipeline_gate import ModelNotAuthorized, require_authorized_gwr
from stage3_validation import DataValidationError, normalize_ibge

BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = BASE_DIR / "static" / "maps"
DEFAULT_TARGET_CRS = "EPSG:31985"


@dataclass
class EpidemiologicalGWROutput:
    joined_data_path: Path | None
    map_paths: dict[str, Path]
    gwr_bandwidth: float
    records_used: int
    dependent_var: str
    independent_vars: list[str]


def _normalize_ibge_code(value: object) -> str:
    """Preserve the full seven-digit official code; never truncate or pad it."""
    return normalize_ibge(value)


def _validate_spatial_inputs(table, municipalities, *, year: int, dependent: str,
                             predictors: list[str], table_code: str, shape_code: str):
    """Validate one cross-section before fitting, without imputation or deduplication."""
    import numpy as np
    import pandas as pd

    if not isinstance(year, int) or isinstance(year, bool) or not 1900 <= year <= 2100:
        raise DataValidationError("An explicit valid analysis year is required")
    if "ano" not in table.columns:
        raise DataValidationError("An explicit 'ano' column is required; wide panels are unsupported")
    if "sexo" in table.columns and table["sexo"].nunique(dropna=False) != 1:
        raise DataValidationError("Select exactly one sex stratum before fitting GWR")
    required = [table_code, "ano", dependent, *predictors]
    if len(set(required)) != len(required) or any(col not in table.columns for col in required):
        raise DataValidationError("Missing or repeated analysis columns")
    if shape_code not in municipalities.columns:
        raise DataValidationError("Municipal geometry has no specified IBGE column")
    if municipalities.crs is None:
        raise DataValidationError("Municipal geometry CRS is unknown; cannot infer it")
    if municipalities.geometry.isna().any() or municipalities.geometry.is_empty.any():
        raise DataValidationError("Missing or empty municipal geometries")
    if not municipalities.geometry.is_valid.all():
        raise DataValidationError("Invalid municipal geometries")
    years = pd.to_numeric(table["ano"], errors="raise")
    if not np.isfinite(years.to_numpy(dtype=float)).all() or not (years == year).all():
        raise DataValidationError("Input must contain only the explicitly selected year")
    tab = table.copy()
    geo = municipalities.copy()
    tab["_ibge_code"] = tab[table_code].map(_normalize_ibge_code)
    geo["_ibge_code"] = geo[shape_code].map(_normalize_ibge_code)
    if tab["_ibge_code"].duplicated().any():
        raise DataValidationError("Duplicate municipality in selected year/sex; no records discarded")
    if geo["_ibge_code"].duplicated().any():
        raise DataValidationError("Duplicate municipality geometries")
    if set(tab["_ibge_code"]) != set(geo["_ibge_code"]):
        missing_geometry = sorted(set(tab["_ibge_code"]) - set(geo["_ibge_code"]))
        missing_data = sorted(set(geo["_ibge_code"]) - set(tab["_ibge_code"]))
        raise DataValidationError(f"Municipality mismatch: without geometry={missing_geometry[:5]}, without observations={missing_data[:5]}")
    for column in [dependent, *predictors]:
        values = pd.to_numeric(tab[column], errors="raise")
        if not np.isfinite(values.to_numpy(dtype=float)).all():
            raise DataValidationError(f"Missing or nonfinite observations in {column}; no imputation")
        tab[column] = values
    if not tab[dependent].between(0, 100).all() and "positividade" in dependent.lower():
        raise DataValidationError("Positivity must be between zero and 100")
    joined = geo.merge(tab[["_ibge_code", dependent, *predictors]], on="_ibge_code", how="inner", validate="one_to_one")
    if len(joined) != len(tab):
        raise DataValidationError("Spatial join unexpectedly changed the number of observations")
    return joined


def _fit_validated_gwr(joined, dependent: str, predictors: list[str], target_crs: str):
    """Fit once with the requested predictors; reject collinearity and invalid diagnostics."""
    import numpy as np
    from mgwr.gwr import GWR
    from mgwr.sel_bw import Sel_BW

    projected = joined.to_crs(target_crs)
    if projected.crs is None or projected.crs.is_geographic:
        raise DataValidationError("GWR requires a valid projected metric CRS")
    if projected.geometry.isna().any() or projected.geometry.is_empty.any():
        raise DataValidationError("Projection produced invalid geometries")
    coords = np.column_stack((projected.geometry.centroid.x, projected.geometry.centroid.y))
    y = projected[[dependent]].to_numpy(dtype=float)
    x = projected[predictors].to_numpy(dtype=float)
    if len(projected) <= len(predictors) + 2:
        raise DataValidationError("Insufficient municipalities for model dimensionality")
    if np.any(np.std(x, axis=0) <= 1e-12) or np.std(y) <= 1e-12:
        raise DataValidationError("Zero-variance outcome or predictor")
    design = np.column_stack((np.ones(len(x)), x))
    if np.linalg.matrix_rank(design) != design.shape[1]:
        raise DataValidationError("Collinear predictors; revise the scientific specification")
    condition = float(np.linalg.cond(design))
    if not math.isfinite(condition) or condition > 30:
        raise DataValidationError(f"Global design condition number too high: {condition}")
    x = (x - x.mean(axis=0)) / x.std(axis=0)
    y = (y - y.mean(axis=0)) / y.std(axis=0)
    bandwidth = Sel_BW(coords, y, x, spherical=False).search()
    if not np.isfinite(bandwidth) or bandwidth <= len(predictors) + 1:
        raise DataValidationError("Invalid or undersized GWR bandwidth")
    result = GWR(coords, y, x, bw=bandwidth, spherical=False).fit()
    for name in ("params", "localR2", "resid_response"):
        values = np.asarray(getattr(result, name), dtype=float)
        if not np.isfinite(values).all():
            raise DataValidationError(f"Nonfinite GWR diagnostic: {name}")
    if not np.isfinite(float(result.aicc)):
        raise DataValidationError("Nonfinite AICc")
    if hasattr(result, "local_collinearity"):
        diagnostics = result.local_collinearity()
        if diagnostics is not None:
            local_conditions = np.asarray(diagnostics[-1], dtype=float)
            if not np.isfinite(local_conditions).all() or (local_conditions > 30).any():
                raise DataValidationError("Local GWR collinearity exceeds threshold")
    return projected, float(bandwidth), result


def generate_epidemiological_gwr_maps(
    tabular_data_path: str | Path,
    municipalities_path: str | Path,
    dependent_var: str,
    independent_vars: list[str],
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    data_ibge_column: str = "codigo_ibge",
    shape_ibge_column: str = "codigo_ibge",
    classification_scheme: str = "natural_breaks",
    n_classes: int = 5,
    target_crs: str = DEFAULT_TARGET_CRS,
    dpi: int = 300,
    title_prefix: str = "Pernambuco - Analise Espacial Epidemiologica",
    save_joined_geodata: bool = True,
    *,
    analysis_year: int | None = None,
) -> EpidemiologicalGWROutput:
    """Fail closed until provenance approval; no files are created on rejection.

    Once authorization is implemented, the spatial and numerical validators above
    must be called before any model output is persisted. Never bypass the gate.
    """
    if analysis_year is None:
        raise DataValidationError("analysis_year is mandatory: choose a single year explicitly")
    if not isinstance(analysis_year, int) or isinstance(analysis_year, bool):
        raise DataValidationError("analysis_year must be an integer")
    if not independent_vars or len(set(independent_vars)) != len(independent_vars):
        raise DataValidationError("Specify distinct independent variables")
    require_authorized_gwr(tabular_data_path, municipalities_path, dependent_var, independent_vars)
    # The authorization function currently always raises. Do not append a
    # reachable fitting path until official data provenance is homologated.
    raise ModelNotAuthorized("Scientific GWR authorization is not implemented")


def main() -> None:
    parser = argparse.ArgumentParser(description="Validated epidemiological GWR (authorization required)")
    parser.add_argument("--table", required=True)
    parser.add_argument("--municipalities", required=True)
    parser.add_argument("--dependent", required=True)
    parser.add_argument("--independent", required=True, nargs="+")
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()
    generate_epidemiological_gwr_maps(args.table, args.municipalities, args.dependent,
                                     args.independent, args.output_dir, analysis_year=args.year)


if __name__ == "__main__":
    main()
