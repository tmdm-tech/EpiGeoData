"""Deterministic validation of municipal epidemiological panels; never authorizes GWR."""
from __future__ import annotations

import csv
import math
import re
from collections import Counter
from pathlib import Path


class DataValidationError(ValueError):
    """Input violates a prerequisite for spatial analysis."""


def normalize_ibge(value: object) -> str:
    """Preserve exactly seven digits; never infer a missing check digit."""
    raw = str(value).strip()
    if not re.fullmatch(r"\d{7}(?:\.0+)?", raw):
        raise DataValidationError(f"Invalid seven-digit IBGE municipality code: {raw!r}")
    return raw.split(".", 1)[0]


def validate_municipal_panel(
    records: list[dict], *, municipality_key: str = "codigo_ibge",
    year_key: str = "ano", outcome_key: str = "positividade_percentual",
    sex_key: str | None = None,
) -> dict:
    """Validate keys and positivity without discarding observations or imputing values."""
    if not records:
        raise DataValidationError("Empty municipal panel")
    keys = []
    years = set()
    municipalities = set()
    for index, record in enumerate(records, 1):
        if not isinstance(record, dict):
            raise DataValidationError(f"Record {index} must be an object")
        try:
            municipality = normalize_ibge(record[municipality_key])
            year_text = str(record[year_key]).strip()
            year = int(year_text)
            outcome = record[outcome_key]
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise DataValidationError(f"Invalid required fields in record {index}") from exc
        if not 1900 <= year <= 2100 or year_text != str(year):
            raise DataValidationError(f"Invalid year in record {index}")
        if outcome is None or str(outcome).strip().casefold() in ("", "-", "...", "nan", "none", "null"):
            raise DataValidationError(f"Missing outcome in record {index}; do not impute")
        try:
            numeric = float(str(outcome).strip().replace(",", "."))
        except (ValueError, TypeError, OverflowError) as exc:
            raise DataValidationError(f"Non-numeric outcome in record {index}") from exc
        if not math.isfinite(numeric) or not 0 <= numeric <= 100:
            raise DataValidationError(f"Nonfinite or out-of-range positivity in record {index}")
        if sex_key is not None and (sex_key not in record or not str(record[sex_key]).strip()):
            raise DataValidationError(f"Missing sex stratum in record {index}")
        key = (municipality, year, str(record[sex_key]).strip() if sex_key else None)
        keys.append(key)
        years.add(year)
        municipalities.add(municipality)
    duplicates = [key for key, count in Counter(keys).items() if count > 1]
    if duplicates:
        raise DataValidationError(f"Duplicate municipality/year/sex keys: {duplicates[:5]}")
    return {"status": "validated", "records": len(records), "municipalities": len(municipalities),
            "years": sorted(years), "sex_disaggregated": sex_key is not None,
            "model_authorized": False, "reason": "Climate alignment, GERES membership and GWR diagnostics not validated"}


def read_panel_csv(path: str | Path, *, delimiter: str = ",") -> list[dict]:
    """Read normalized long-format CSV, never interpret the wide PCE file implicitly."""
    with Path(path).open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle, delimiter=delimiter))
