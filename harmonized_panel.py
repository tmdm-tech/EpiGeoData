"""Build a provenance-aware municipal panel without imputing or inventing observations.

Inputs are already-observed long-format records and a verified official IBGE/GERES
crosswalk. A six-digit municipality identifier cannot be repaired automatically.
"""
from __future__ import annotations

import csv
import json
import math
import re
from pathlib import Path


class HarmonizationError(ValueError):
    """A source, observation or spatial/temporal alignment cannot be verified."""


def _code(value: object) -> str:
    text = str(value).strip()
    if not re.fullmatch(r"\d{7}(?:\.0+)?", text):
        raise HarmonizationError(f"Official seven-digit IBGE code required: {text!r}")
    return text.split(".", 1)[0]


def _year(value: object) -> int:
    text = str(value).strip()
    if not re.fullmatch(r"\d{4}", text) or not 1900 <= int(text) <= 2100:
        raise HarmonizationError(f"Invalid or missing year: {text!r}")
    return int(text)


def _source(record: dict, kind: str) -> dict:
    provenance = record.get("provenance")
    if not isinstance(provenance, dict):
        raise HarmonizationError(f"{kind}: structured provenance is required")
    required = ("organization", "dataset", "version", "retrieved_at", "reference_url", "method")
    if any(not isinstance(provenance.get(key), str) or not provenance[key].strip() for key in required):
        raise HarmonizationError(f"{kind}: incomplete provenance; required: {', '.join(required)}")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", provenance["retrieved_at"]):
        raise HarmonizationError(f"{kind}: retrieved_at must be YYYY-MM-DD")
    if not provenance["reference_url"].startswith(("https://", "http://")):
        raise HarmonizationError(f"{kind}: source URL required")
    if provenance.get("verified") is not True:
        raise HarmonizationError(f"{kind}: source must be explicitly verified")
    return {key: provenance[key] for key in required}


def _number(value: object, field: str) -> float:
    if isinstance(value, bool) or value is None or str(value).strip() in ("", "-", "..."):
        raise HarmonizationError(f"Missing/non-numeric {field}; no imputation allowed")
    try:
        result = float(str(value).strip().replace(",", "."))
    except (ValueError, TypeError, OverflowError) as exc:
        raise HarmonizationError(f"Non-numeric {field}") from exc
    if not math.isfinite(result):
        raise HarmonizationError(f"Non-finite {field}")
    return result


def _unique(rows: list[dict], keys: tuple[str, ...], label: str) -> dict[tuple, dict]:
    indexed = {}
    for row in rows:
        try:
            key = tuple(row[field] for field in keys)
        except (KeyError, TypeError) as exc:
            raise HarmonizationError(f"Missing {label} key: {exc}") from exc
        if key in indexed:
            raise HarmonizationError(f"Duplicate {label} key: {key}")
        indexed[key] = row
    return indexed


def harmonize(
    epidemiology: list[dict], climate: list[dict], territories: list[dict],
    *, expected_years: list[int], expected_municipalities: list[str],
    outcome_field: str = "positividade_percentual", climate_field: str = "temperatura_c",
    climate_fields: list[str] | None = None,
) -> list[dict]:
    """Strict 1:1 municipality-year-stratum join; climate is municipality-year.

    Epidemiology requires municipio_ibge, ano, estrato, outcome and provenance;
    climate requires municipio_ibge, ano, climate variable and provenance;
    territory requires municipio_ibge, geres and provenance. Coverage must match
    the explicitly requested municipality/year grid. No silent inner joins.
    """
    if not epidemiology or not climate or not territories or not expected_years or not expected_municipalities:
        raise HarmonizationError("All three sources and explicit coverage are required")
    years = {_year(value) for value in expected_years}
    codes = {_code(value) for value in expected_municipalities}
    if len(years) != len(expected_years) or len(codes) != len(expected_municipalities):
        raise HarmonizationError("Duplicate expected years or municipalities")
    territorial = []
    for raw in territories:
        code = _code(raw["municipio_ibge"])
        geres = str(raw.get("geres", "")).strip()
        if not geres:
            raise HarmonizationError(f"Missing official GERES membership for {code}")
        territorial.append({"municipio_ibge": code, "geres": geres, "provenance": _source(raw, "territory")})
    territory_index = _unique(territorial, ("municipio_ibge",), "territory")
    # _unique always returns tuple keys; compare like-for-like without altering IBGE codes.
    territory_codes = {key[0] for key in territory_index}
    if territory_codes != codes:
        raise HarmonizationError(f"Territorial mismatch: missing={sorted(codes-territory_codes)}, unexpected={sorted(territory_codes-codes)}")
    climatic = []
    requested_climate_fields = climate_fields or [climate_field]
    if not requested_climate_fields:
        raise HarmonizationError("At least one environmental predictor is required")
    for raw in climate:
        code, year = _code(raw["municipio_ibge"]), _year(raw["ano"])
        values = {field: _number(raw.get(field), field) for field in requested_climate_fields}
        climatic.append({"municipio_ibge": code, "ano": year, **values, "provenance": _source(raw, "climate")})
    climate_index = _unique(climatic, ("municipio_ibge", "ano"), "climate")
    expected = {(code, year) for code in codes for year in years}
    if set(climate_index) != expected:
        raise HarmonizationError(f"Climate municipality/year mismatch: missing={sorted(expected-set(climate_index))[:10]}, unexpected={sorted(set(climate_index)-expected)[:10]}")
    epidemiological = []
    for raw in epidemiology:
        code, year = _code(raw["municipio_ibge"]), _year(raw["ano"])
        stratum = str(raw.get("estrato", "")).strip()
        if not stratum:
            raise HarmonizationError("Explicit epidemiological stratum required")
        epidemiological.append({"municipio_ibge": code, "ano": year, "estrato": stratum, outcome_field: _number(raw.get(outcome_field), outcome_field), "provenance": _source(raw, "epidemiology")})
    epi_index = _unique(epidemiological, ("municipio_ibge", "ano", "estrato"), "epidemiology")
    strata = {row["estrato"] for row in epidemiological}
    expected_epi = {(code, year, stratum) for code in codes for year in years for stratum in strata}
    if set(epi_index) != expected_epi:
        raise HarmonizationError(f"Epidemiological municipality/year/stratum mismatch: missing={sorted(expected_epi-set(epi_index))[:10]}, unexpected={sorted(set(epi_index)-expected_epi)[:10]}")
    result = []
    for code, year, stratum in sorted(expected_epi):
        epi = epi_index[(code, year, stratum)]
        clim = climate_index[(code, year)]
        territory = territory_index[(code,)]
        result.append({"municipio_ibge": code, "ano": year, "estrato": stratum, "geres": territory["geres"], outcome_field: epi[outcome_field], **{field: clim[field] for field in requested_climate_fields}, "epidemiology_provenance": epi["provenance"], "climate_provenance": clim["provenance"], "territory_provenance": territory["provenance"]})
    return result


def load_json_records(path: str | Path) -> list[dict]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(data, list) or any(not isinstance(row, dict) for row in data):
        raise HarmonizationError("Input must be a JSON array of records")
    return data


def export_panel(rows: list[dict], path: str | Path) -> None:
    """Write only a successfully harmonized panel, retaining all source metadata."""
    if not rows:
        raise HarmonizationError("Refusing to export empty panel")
    destination = Path(path)
    if destination.exists():
        raise HarmonizationError("Refusing to overwrite an existing panel")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("x", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]), extrasaction="raise")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(value, ensure_ascii=False, sort_keys=True) if isinstance(value, dict) else value for key, value in row.items()})
