"""Read-only metadata for explicitly registered EpiGeoData datasets.

Catalogue presence and decoding are not scientific validation or permission to run GWR.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent

DATASETS = (
    {"id": "pce_positividade", "disease": "esquistossomose", "path": "Esquistossomose.csv", "kind": "epidemiological", "indicator": "positividade_percentual", "years": [2001, 2023], "sex_disaggregated": False, "scope": "Pernambuco", "provenance": "PCE - Programa de Controle da Esquistossomose; cabeçalho do arquivo"},
    {"id": "dengue_casos_provaveis", "disease": "dengue", "path": "Dengue.csv", "kind": "epidemiological", "indicator": "casos_provaveis_por_municipio_notificacao", "years": [2014, 2026], "sex_disaggregated": False, "scope": "Pernambuco", "provenance": "SINAN; cabeçalho do arquivo; ano de notificação, não residência"},
    {"id": "chikungunya_casos", "disease": "chikungunya", "path": "Chikungunya.csv", "kind": "epidemiological", "indicator": "casos_por_municipio_notificacao", "years": [2017, 2026], "sex_disaggregated": False, "scope": "Pernambuco", "provenance": "SINAN; cabeçalho do arquivo; ano de notificação, não residência"},
    {"id": "scz_eventos", "disease": "scz", "path": "SCZ.csv", "kind": "epidemiological", "indicator": "eventos_por_municipio_notificacao", "years": [2015, 2024], "sex_disaggregated": False, "scope": "Pernambuco", "provenance": "RESP-Microcefalia; cabeçalho do arquivo; município e ano de notificação"},
    {"id": "tuberculose_confirmados", "disease": "tuberculose", "path": "Tuberculose.csv", "kind": "epidemiological", "indicator": "casos_confirmados_por_municipio_notificacao", "years": [2001, 2024], "sex_disaggregated": False, "scope": "Pernambuco", "provenance": "SINAN; cabeçalho do arquivo; município de notificação e ano de diagnóstico"},
    {"id": "temperatura_pontos", "path": "data/climaticas/temperatura.geojson", "kind": "climate", "indicator": "temperatura_c", "sex_disaggregated": False, "scope": "pontos observados no arquivo", "provenance": "GeoJSON versionado no repositório; origem instrumental não documentada"},
    {"id": "malha_municipal", "path": "data/municipios_pe_ibge.geojson", "kind": "cartography", "indicator": None, "scope": "Pernambuco", "provenance": "arquivo municipal versionado no repositório; validar versão oficial"},
    {"id": "epidemiologia_demo", "path": "data/epidemiologia_demo_pe.csv", "kind": "demonstration", "indicator": "não validado", "scope": "Pernambuco", "provenance": "arquivo identificado como demonstração; não usar como observação real"},
)


def _csv_header(path: Path) -> tuple[list[str], str]:
    """Decode legacy CSV without replacement characters; find its municipality header."""
    raw = path.read_bytes()
    try:
        content, encoding = raw.decode("utf-8-sig"), "utf-8-sig"
    except UnicodeDecodeError:
        content, encoding = raw.decode("cp1252"), "cp1252"
    for line in content.splitlines():
        if line.lstrip().startswith('"Município'):
            header = next(csv.reader([line], delimiter=";"))
            if len(header) < 3 or not header[0].startswith("Município"):
                raise ValueError("Invalid municipality CSV header")
            return header, encoding
    raise ValueError("Municipality CSV header not found")


def catalogue(root: Path = ROOT) -> list[dict]:
    """Report every registered dataset independently, without inventing observations."""
    output = []
    for item in DATASETS:
        path = root / item["path"]
        exists = path.is_file()
        entry = {**item, "available": exists, "bytes": path.stat().st_size if exists else None}
        if exists and item["id"] == "temperatura_pontos":
            try:
                features = json.loads(path.read_text(encoding="utf-8"))["features"]
                entry["observations"] = len(features)
                entry["municipalities_observed"] = sorted({str(f.get("properties", {}).get("municipio_nome", "")) for f in features})
                entry["dates_observed"] = sorted({str(f.get("properties", {}).get("data", "")) for f in features})
            except (ValueError, KeyError, TypeError, OSError) as exc:
                entry["read_error"] = type(exc).__name__
        if exists and item.get("kind") == "epidemiological":
            try:
                entry["columns"], entry["encoding"] = _csv_header(path)
            except (OSError, ValueError, UnicodeError) as exc:
                entry["read_error"] = type(exc).__name__
        output.append(entry)
    return output


def assess_analysis(disease: str, territory: str, sex: str, climate: str, method: str, root: Path = ROOT) -> dict:
    """Fail closed: data inventory does not validate disease-specific models."""
    sources = {entry["id"]: entry for entry in catalogue(root)}
    issues = []
    disease_key = disease.strip().casefold()
    disease_sources = [entry for entry in sources.values() if entry.get("disease") == disease_key]
    if not disease_sources:
        issues.append(f"Fonte epidemiológica para {disease} ainda não cadastrada no catálogo.")
    for entry in disease_sources:
        if not entry["available"]:
            issues.append(f"Fonte obrigatória ausente: {entry['id']}.")
        if entry.get("read_error"):
            issues.append(f"Fonte ilegível: {entry['id']}.")
    for key in ("temperatura_pontos", "malha_municipal"):
        if not sources[key]["available"]:
            issues.append(f"Fonte obrigatória ausente: {key}.")
        if sources[key].get("read_error"):
            issues.append(f"Fonte ilegível: {key}.")
    issues.extend([
        "Vinculação oficial e versionada dos códigos IBGE à região solicitada não validada.",
        "Dados epidemiológicos e climáticos alinhados por município e período não validados.",
        "Indicador, população de referência, denominadores e unidade territorial devem ser validados para cada doença.",
        "Ajuste GWR exige verificar amostra, variabilidade, banda, colinearidade e resíduos antes da execução.",
    ])
    return {"status": "blocked", "can_run_gwr": False, "configuration": {"disease": disease, "territory": territory, "sex": sex, "climate": climate, "method": method}, "issues": issues, "sources": list(sources.values()), "results": None}
