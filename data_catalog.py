"""Read-only catalogue for datasets shipped with EpiGeoData.

This module does not download data, infer missing measurements or run a model.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent

DATASETS = (
    {"id": "pce_positividade", "path": "Esquistossomose.csv", "kind": "epidemiological", "indicator": "positividade_percentual", "years": [2001, 2023], "sex_disaggregated": False, "scope": "Pernambuco", "provenance": "PCE - Programa de Controle da Esquistossomose; cabeçalho do arquivo"},
    {"id": "temperatura_pontos", "path": "data/climaticas/temperatura.geojson", "kind": "climate", "indicator": "temperatura_c", "sex_disaggregated": False, "scope": "pontos observados no arquivo", "provenance": "GeoJSON versionado no repositório; origem instrumental não documentada"},
    {"id": "malha_municipal", "path": "data/municipios_pe_ibge.geojson", "kind": "cartography", "indicator": None, "scope": "Pernambuco", "provenance": "arquivo municipal versionado no repositório; validar versão oficial"},
    {"id": "epidemiologia_demo", "path": "data/epidemiologia_demo_pe.csv", "kind": "demonstration", "indicator": "não validado", "scope": "Pernambuco", "provenance": "arquivo identificado como demonstração; não usar como observação real"},
)


def _pce_header(path: Path) -> tuple[list[str], str]:
    """Read only the PCE header, with an explicit fallback for legacy CSV bytes.

    A decoding fallback does not establish the dataset's scientific provenance.
    """
    raw = path.read_bytes()
    try:
        content = raw.decode("utf-8-sig")
        encoding = "utf-8-sig"
    except UnicodeDecodeError:
        content = raw.decode("cp1252")
        encoding = "cp1252"
    for line in content.splitlines():
        if line.lstrip().startswith('"Município"'):
            header = next(csv.reader([line], delimiter=";"))
            if len(header) < 3 or header[0] != "Município":
                raise ValueError("Invalid PCE header")
            return header, encoding
    raise ValueError("PCE header not found")


def catalogue(root: Path = ROOT) -> list[dict]:
    """Return file-backed metadata; missing files are never silently accepted."""
    output = []
    for item in DATASETS:
        path = root / item["path"]
        entry = {**item, "available": path.is_file(), "bytes": path.stat().st_size if path.is_file() else None}
        if entry["available"] and item["id"] == "temperatura_pontos":
            try:
                features = json.loads(path.read_text(encoding="utf-8"))["features"]
                entry["observations"] = len(features)
                entry["municipalities_observed"] = sorted({str(f.get("properties", {}).get("municipio_nome", "")) for f in features})
                entry["dates_observed"] = sorted({str(f.get("properties", {}).get("data", "")) for f in features})
            except (ValueError, KeyError, TypeError, OSError) as exc:
                entry["read_error"] = type(exc).__name__
        if entry["available"] and item["id"] == "pce_positividade":
            try:
                entry["columns"], entry["encoding"] = _pce_header(path)
            except (OSError, ValueError, UnicodeError) as exc:
                entry["read_error"] = type(exc).__name__
        output.append(entry)
    return output


def assess_analysis(disease: str, territory: str, sex: str, climate: str, method: str, root: Path = ROOT) -> dict:
    """Fail closed until municipal membership, aligned observations and GWR diagnostics are validated."""
    sources = {entry["id"]: entry for entry in catalogue(root)}
    issues = []
    supported = (disease.casefold() == "esquistossomose" and territory.casefold() == "v geres" and sex.casefold() == "todos" and climate.casefold() == "temperatura" and method.casefold() == "gwr")
    if not supported:
        issues.append("Configuração ainda não homologada pelo catálogo de análises.")
    for key in ("pce_positividade", "temperatura_pontos", "malha_municipal"):
        if not sources[key]["available"]:
            issues.append(f"Fonte obrigatória ausente: {key}.")
        if sources[key].get("read_error"):
            issues.append(f"Fonte ilegível: {key}.")
    issues.extend([
        "Vinculação oficial e versionada dos códigos IBGE à V GERES não validada.",
        "Temperatura municipal para os mesmos municípios e períodos do desfecho não validada.",
        "Positividade do PCE não equivale a taxa de detecção; indicador e denominadores devem ser explicitados.",
        "Ajuste GWR exige verificar amostra, variabilidade, banda, colinearidade e resíduos antes da execução.",
    ])
    return {"status": "blocked", "can_run_gwr": False, "configuration": {"disease": disease, "territory": territory, "sex": sex, "climate": climate, "method": method}, "issues": issues, "sources": list(sources.values()), "results": None}
