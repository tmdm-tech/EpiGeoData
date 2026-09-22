"""Mandatory safety gate for legacy spatial modelling entrypoints.

Validation is deliberately independent of Flask: CLI and direct Python callers must
not bypass the restrictions imposed on the HTTP endpoints. No data are invented.
"""
from __future__ import annotations

from pathlib import Path

from stage3_validation import DataValidationError, read_panel_csv, validate_municipal_panel


class ModelNotAuthorized(DataValidationError):
    """A model must not run before its data and scientific protocol are approved."""


def require_authorized_gwr(
    tabular_data_path: str | Path,
    municipalities_path: str | Path,
    dependent_var: str,
    independent_vars: list[str],
) -> None:
    """Fail closed before creating outputs or loading large geospatial dependencies.

    The legacy generator lacks a period/sex-aware spatial join, official GERES
    provenance and validated model diagnostics. Merely passing a syntactically
    valid CSV must never turn these missing controls into model authorization.
    """
    path = Path(tabular_data_path)
    if not path.is_file():
        raise ModelNotAuthorized(f"Epidemiological source unavailable: {path}")
    if not Path(municipalities_path).is_file():
        raise ModelNotAuthorized("Municipal geometry source unavailable")
    if not dependent_var or not independent_vars:
        raise ModelNotAuthorized("Dependent and independent variables are required")
    if path.suffix.lower() == ".csv":
        try:
            panel = read_panel_csv(path)
            assessment = validate_municipal_panel(panel, outcome_key=dependent_var)
        except (OSError, UnicodeError, DataValidationError) as exc:
            raise ModelNotAuthorized(f"Panel validation failed: {exc}") from exc
        if assessment["model_authorized"]:
            raise ModelNotAuthorized("Unexpected authorization from a preliminary validator")
    raise ModelNotAuthorized(
        "GWR blocked: official GERES/IBGE provenance, municipality-period climate "
        "alignment, spatial join grain and statistical diagnostics are not homologated"
    )
