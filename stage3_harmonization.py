"""Lossless, explicit join validation for municipality-period observations.

No 6-to-7-digit IBGE conversion, temporal aggregation or missing-data imputation.
"""
from __future__ import annotations

from collections import Counter
from stage3_validation import DataValidationError, normalize_ibge


def validate_spatial_keys(panel: list[dict], geometries: list[dict], *,
                          municipality_key: str = 'codigo_ibge',
                          year_key: str = 'ano', sex_key: str | None = None) -> dict:
    """Audit join cardinality without merging or silently dropping records."""
    if not panel or not geometries:
        raise DataValidationError('Panel and geometry must both be nonempty')
    geometry_codes = [normalize_ibge(row[municipality_key]) for row in geometries]
    duplicates = [code for code, count in Counter(geometry_codes).items() if count > 1]
    if duplicates:
        raise DataValidationError(f'Duplicate municipality geometries: {duplicates[:5]}')
    geometry_set = set(geometry_codes)
    panel_codes = [normalize_ibge(row[municipality_key]) for row in panel]
    missing_geometry = sorted(set(panel_codes) - geometry_set)
    if missing_geometry:
        raise DataValidationError(f'Panel municipalities missing geometry: {missing_geometry[:10]}')
    missing_panel = sorted(geometry_set - set(panel_codes))
    if missing_panel:
        raise DataValidationError(f'Geometry municipalities missing panel observations: {missing_panel[:10]}')
    keys = []
    for row in panel:
        year = str(row.get(year_key, '')).strip()
        if not year.isdigit() or len(year) != 4:
            raise DataValidationError('Each panel row requires an explicit four-digit year')
        if sex_key and not str(row.get(sex_key, '')).strip():
            raise DataValidationError('Sex stratum missing')
        keys.append((normalize_ibge(row[municipality_key]), year,
                     str(row[sex_key]).strip() if sex_key else None))
    if len(set(keys)) != len(keys):
        raise DataValidationError('Duplicate municipality-period-sex observations')
    return {'status': 'join_keys_validated', 'panel_records': len(panel),
            'geometry_municipalities': len(geometry_codes),
            'years': sorted({key[1] for key in keys}),
            'model_authorized': False,
            'reason': 'Source provenance, climate alignment and GWR diagnostics still pending'}
