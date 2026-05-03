import csv
import io
import json
import os
import re
import zipfile
from functools import lru_cache
from pathlib import Path
from datetime import datetime
from urllib.parse import urlencode
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

from flask import Flask, jsonify, render_template, request, send_file
import geopandas as gpd
import pandas as pd
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from werkzeug.utils import secure_filename

app = Flask(__name__)

REALTIME_CACHE: dict[str, tuple[float, dict]] = {}
REALTIME_CACHE_TTL_SECONDS = 20 * 60
DEFAULT_PROFESSIONAL_MAP_TITLE = "EpiGeoData | Mapa Coropletico Cientifico - Pernambuco"
DEFAULT_PREPARED_HEATMAP_FILE = "municpios_pe"
DEFAULT_PERNAMBUCO_CARTOGRAPHY = Path(__file__).parent / "data" / "municipios_pe_ibge.geojson"
CLIMATE_SOURCE_BINDINGS = {
    "precipitacao": [
        "Precipitacao_INMET_ANA-20260416T16124",
        "Precipitação_INMET_ANA-20260416T16124",
    ],
    "cobertura_vegetal": [
        "states_caatinga_biome.zip",
        "residual_biome_caatinga_v20260330.zip",
        "conservation_units_caatinga_biome.zip",
    ],
    "relevo_hidrografia": [
        "Instalador Hidro Build 1.4.0.83.zip",
    ],
}

CLIMATE_LAYER_BINDINGS = {
    "precipitacao": "precipitacao",
    "temperatura": "temperatura",
    "cobertura_vegetal": "cobertura_vegetal",
    "relevo_hidrografia": "queimadas",
}

DISEASE_CATALOG = {
        "scz": {
            "display_name": "Sindrome Congenita da Zika",
            "aliases": ["scz", "sindrome_congenita_da_zika", "sindrome_congenita_zika", "zika"],
            "csv_aliases": ["scz", "sindrome_congenita_da_zika", "sindrome_congenita_zika", "zika"],
            "datasus_page": "https://datasus.saude.gov.br/acesso-a-informacao/registro-de-eventos-em-saude-publica-resp-microcefalia/",
            "datasus_tabnet": "http://tabnet.datasus.gov.br/cgi/tabcgi.exe?resp/cnv/resp",
        },
        "covid_19": {
            "display_name": "Covid 19",
            "aliases": ["covid_19", "covid19", "covid", "srag_covid"],
            "csv_aliases": ["covid_19", "covid19", "covid"],
            "datasus_page": "https://opendatasus.saude.gov.br/",
            "datasus_tabnet": None,
        },
        "dengue": {
            "display_name": "Dengue",
            "aliases": ["dengue"],
            "csv_aliases": ["dengue"],
            "datasus_page": "https://datasus.saude.gov.br/acesso-a-informacao/doencas-e-agravos-de-notificacao-de-2007-em-diante-sinan/",
            "datasus_tabnet": "http://tabnet.datasus.gov.br/cgi/deftohtm.exe?sinannet/cnv/dengue",
        },
        "esquistossomose": {
            "display_name": "Esquistossomose",
            "aliases": ["esquistossomose", "esquisto"],
            "csv_aliases": ["esquistossomose"],
            "datasus_page": "https://datasus.saude.gov.br/acesso-a-informacao/programa-de-controle-da-esquistossomose-pce/",
            "datasus_tabnet": "http://tabnet.datasus.gov.br/cgi/deftohtm.exe?sinan/pce/cnv/pce",
        },
        "tuberculose": {
            "display_name": "Tuberculose",
            "aliases": ["tuberculose", "tuberc"],
            "csv_aliases": ["tuberculose"],
            "datasus_page": "https://datasus.saude.gov.br/acesso-a-informacao/tuberculose-desde-2001-sinan/",
            "datasus_tabnet": "http://tabnet.datasus.gov.br/cgi/tabcgi.exe?sinannet/cnv/tuberc",
        },
        "monkeypox": {
            "display_name": "Monkeypox",
            "aliases": ["monkeypox", "mpox"],
            "csv_aliases": ["monkeypox", "mpox"],
            "datasus_page": "https://datasus.saude.gov.br/acesso-a-informacao/doencas-e-agravos-de-notificacao-de-2007-em-diante-sinan/",
            "datasus_tabnet": None,
        },
        "chikungunya": {
            "display_name": "Chikungunya",
            "aliases": ["chikungunya", "chikun"],
            "csv_aliases": ["chikungunya"],
            "datasus_page": "https://datasus.saude.gov.br/acesso-a-informacao/doencas-e-agravos-de-notificacao-de-2007-em-diante-sinan/",
            "datasus_tabnet": "http://tabnet.datasus.gov.br/cgi/deftohtm.exe?sinannet/cnv/chikun",
        },
        "oropouche": {
            "display_name": "Febre Oropouche",
            "aliases": ["oropouche", "febre_oropouche"],
            "csv_aliases": ["oropouche", "febre_oropouche"],
            "datasus_page": "https://datasus.saude.gov.br/acesso-a-informacao/doencas-e-agravos-de-notificacao-de-2007-em-diante-sinan/",
            "datasus_tabnet": None,
        },
}

DISEASE_FILE_ALIASES = {
    key: meta["csv_aliases"]
    for key, meta in DISEASE_CATALOG.items()
}

DATASUS_CATALOG_CACHE: dict[str, tuple[float, dict]] = {}
DATASUS_CATALOG_TTL_SECONDS = 6 * 60 * 60


def _resolve_climate_source_file(filename: str) -> Path | None:
    candidates = [
        Path(__file__).parent,
        Path(__file__).parent / "data",
        Path(__file__).parent / "data" / "climaticas",
        Path(__file__).parent / "static",
    ]
    for base in candidates:
        if not base.exists() or not base.is_dir():
            continue
        direct = base / filename
        if direct.exists() and direct.is_file():
            return direct

        token = _normalize_token(Path(filename).stem)
        for path in base.rglob("*"):
            if not path.is_file():
                continue
            if _normalize_token(path.stem) == token:
                return path

    return None

DISEASE_FILE_ALIASES = {
    "dengue": ["dengue"],
    "esquistossomose": ["esquistossomose"],
    "tuberculose": ["tuberculose"],
    "chikungunya": ["chikungunya"],
    "scz": ["scz", "sindrome_congenita_da_zika", "sindrome_congenita_zika", "zika"],
}


def _normalize_token(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"\s+", "_", value)
    value = re.sub(r"[^a-z0-9_]+", "", value)
    return value


def _normalize_municipio_key(value: str) -> str:
    value = str(value or "").strip().lower()
    value = value.replace("ç", "c")
    value = re.sub(r"[áàâãä]", "a", value)
    value = re.sub(r"[éèêë]", "e", value)
    value = re.sub(r"[íìîï]", "i", value)
    value = re.sub(r"[óòôõö]", "o", value)
    value = re.sub(r"[úùûü]", "u", value)
    value = re.sub(r"\s+", " ", value)
    return value


def _normalize_ibge_code(raw: str | int | float) -> str:
    digits = re.sub(r"\D+", "", str(raw or ""))
    if not digits:
        return ""
    if len(digits) >= 7:
        return digits[:7]
    return digits.zfill(7)


def _normalize_header(value: str) -> str:
    value = str(value or "").strip().lower()
    value = re.sub(r"\s+", "_", value)
    value = re.sub(r"[^a-z0-9_]+", "", value)
    return value


def _resolve_disease_key(disease_key: str) -> str | None:
    token = _normalize_token(str(disease_key or ""))
    if token in DISEASE_CATALOG:
        return token

    for key, meta in DISEASE_CATALOG.items():
        aliases = {_normalize_token(alias) for alias in meta.get("aliases", [])}
        if token in aliases:
            return key

    return None


def _resolve_prepared_heatmap_file(filename: str = DEFAULT_PREPARED_HEATMAP_FILE) -> Path | None:
    candidates = [
        Path(__file__).parent,
        Path(__file__).parent / "data",
        Path(__file__).parent / "data" / "climaticas",
        Path(__file__).parent / "static",
    ]
    variants = [
        filename,
        f"{filename}.csv",
        f"{filename}.xlsx",
        f"{filename}.xls",
        "municipios_pe",
        "municipios_pe.csv",
        "municipios_pe.xlsx",
        "municipios_pe.xls",
    ]

    for base in candidates:
        if not base.exists() or not base.is_dir():
            continue
        for variant in variants:
            candidate = base / variant
            if candidate.exists() and candidate.is_file():
                return candidate

    return None


def _resolve_disease_csv_path(disease_key: str) -> Path | None:
    resolved_key = _resolve_disease_key(disease_key)
    token = resolved_key or _normalize_token(disease_key)
    aliases = DISEASE_FILE_ALIASES.get(token, [token])

    candidates = [
        Path(__file__).parent / "data" / "doencas",
        Path(__file__).parent / "data",
        Path(__file__).parent,
    ]

    for base in candidates:
        if not base.exists() or not base.is_dir():
            continue
        for path in base.rglob("*.csv"):
            stem = _normalize_token(path.stem)
            if stem in aliases:
                return path

    return None


def _probe_remote_source(url: str | None, timeout: int = 6) -> dict[str, object]:
    if not url:
        return {"url": None, "status": "indisponivel"}

    try:
        req = Request(url, headers={"User-Agent": "EpiGeoData/1.0"})
        with urlopen(req, timeout=timeout) as response:
            return {
                "url": url,
                "status": "disponivel",
                "http_status": getattr(response, "status", 200),
            }
    except (URLError, HTTPError, TimeoutError) as error:
        return {
            "url": url,
            "status": "indisponivel",
            "http_status": getattr(error, "code", None),
            "error": str(error),
        }


def _get_datasus_live_status(disease_key: str) -> dict[str, object]:
    resolved_key = _resolve_disease_key(disease_key)
    if not resolved_key:
        return {
            "checked_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "available": False,
            "sources": [],
        }

    cached = DATASUS_CATALOG_CACHE.get(resolved_key)
    now_ts = datetime.utcnow().timestamp()
    if cached and (now_ts - cached[0] <= DATASUS_CATALOG_TTL_SECONDS):
        return cached[1]

    meta = DISEASE_CATALOG[resolved_key]
    sources = [
        _probe_remote_source(meta.get("datasus_page")),
        _probe_remote_source(meta.get("datasus_tabnet")),
    ]
    payload = {
        "checked_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "available": any(item.get("status") == "disponivel" for item in sources if item.get("url")),
        "sources": [item for item in sources if item.get("url")],
    }
    DATASUS_CATALOG_CACHE[resolved_key] = (now_ts, payload)
    return payload


@lru_cache(maxsize=1)
def _load_pernambuco_cartography() -> tuple[gpd.GeoDataFrame, dict, dict, list[dict[str, str]], Path]:
    if not DEFAULT_PERNAMBUCO_CARTOGRAPHY.exists():
        raise FileNotFoundError(
            f"Cartografia de Pernambuco nao encontrada em {DEFAULT_PERNAMBUCO_CARTOGRAPHY.relative_to(Path(__file__).parent)}"
        )

    gdf = gpd.read_file(DEFAULT_PERNAMBUCO_CARTOGRAPHY)
    gdf = gdf.copy()
    gdf["code_muni"] = gdf["code_muni"].map(_normalize_ibge_code)
    gdf["name_muni"] = gdf["name_muni"].astype(str).str.strip()

    municipalities_geojson = json.loads(gdf.to_json())
    state_gdf = gdf[["geometry"]].dissolve()
    state_gdf["name"] = "Pernambuco"
    state_geojson = json.loads(state_gdf.to_json())
    catalog = [
        {"id": row["code_muni"], "nome": row["name_muni"]}
        for _, row in gdf[["code_muni", "name_muni"]].sort_values("name_muni").iterrows()
    ]
    return gdf, municipalities_geojson, state_geojson, catalog, DEFAULT_PERNAMBUCO_CARTOGRAPHY


def _build_disease_payload(disease_key: str, include_municipios: bool = True) -> dict[str, object] | None:
    resolved_key = _resolve_disease_key(disease_key)
    if not resolved_key:
        return None

    meta = DISEASE_CATALOG[resolved_key]
    values, path = _load_disease_csv(resolved_key)
    nums = list(values.values()) if values else []
    datasus_live = _get_datasus_live_status(resolved_key)
    local_source = str(path.relative_to(Path(__file__).parent)) if path else None

    payload: dict[str, object] = {
        "disease": resolved_key,
        "display_name": meta["display_name"],
        "source": local_source,
        "source_label": (
            f"DATASUS ao vivo + fallback local ({local_source})"
            if local_source
            else "DATASUS ao vivo sem serie municipal local"
        ),
        "mode": (
            "datasus_live_with_local_fallback"
            if local_source
            else "datasus_live_metadata_only"
        ),
        "datasus": {
            "page": meta.get("datasus_page"),
            "tabnet": meta.get("datasus_tabnet"),
            "live_status": datasus_live,
        },
        "summary": {
            "total_municipios": len(nums),
            "min": min(nums) if nums else None,
            "max": max(nums) if nums else None,
            "mean": round(sum(nums) / len(nums), 4) if nums else None,
        },
    }
    if include_municipios:
        payload["municipios"] = values or {}
    return payload


def _resolve_workspace_file(path_value: str) -> Path | None:
    candidate = Path(path_value)
    if candidate.is_absolute() and candidate.exists() and candidate.is_file():
        return candidate

    relative = Path(__file__).parent / candidate
    if relative.exists() and relative.is_file():
        return relative
    return None


def _save_uploaded_file(upload, destination_dir: Path, prefix: str) -> Path:
    safe_name = secure_filename(upload.filename or "upload.bin")
    saved_path = destination_dir / f"{prefix}_{safe_name}"
    upload.save(saved_path)
    return saved_path


def _resolve_uploaded_municipalities_file(uploaded_path: Path, extraction_dir: Path) -> Path:
    suffix = uploaded_path.suffix.lower()
    if suffix in {".geojson", ".json", ".gpkg", ".shp"}:
        return uploaded_path

    if suffix != ".zip":
        raise ValueError("Arquivo geoespacial invalido. Use GeoJSON, SHP ou ZIP contendo shapefile.")

    extraction_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(uploaded_path, "r") as zip_ref:
        zip_ref.extractall(extraction_dir)

    geojson_candidates = sorted(extraction_dir.rglob("*.geojson"))
    if geojson_candidates:
        return geojson_candidates[0]

    shp_candidates = sorted(extraction_dir.rglob("*.shp"))
    if shp_candidates:
        for shp_path in shp_candidates:
            base = shp_path.with_suffix("")
            missing_parts = [
                suffix
                for suffix in (".dbf", ".shx")
                if not (base.with_suffix(suffix)).exists()
            ]
            if missing_parts:
                missing_text = ", ".join(missing_parts)
                raise ValueError(
                    "ZIP com shapefile incompleto. Faltam arquivos obrigatorios para "
                    f"{shp_path.name}: {missing_text}"
                )
        return shp_candidates[0]

    raise ValueError("ZIP enviado nao contem arquivo .shp ou .geojson.")


def _parse_independent_vars_form(form) -> list[str]:
    listed = [str(value).strip() for value in form.getlist("independent_vars") if str(value).strip()]
    if listed:
        return listed

    csv_value = str(form.get("independent_vars", "")).strip()
    if not csv_value:
        return []
    return [item.strip() for item in csv_value.split(",") if item.strip()]


def _build_gwr_api_response(result, static_root: Path) -> dict[str, object]:
    timestamp = int(datetime.utcnow().timestamp())
    maps = {
        key: f"/static/{path.relative_to(static_root).as_posix()}?v={timestamp}"
        for key, path in result.map_paths.items()
    }

    response: dict[str, object] = {
        "ok": True,
        "dependent_var": result.dependent_var,
        "independent_vars": result.independent_vars,
        "records_used": result.records_used,
        "gwr_bandwidth": result.gwr_bandwidth,
        "maps": maps,
    }
    if result.joined_data_path is not None:
        response["joined_geojson"] = (
            f"/static/{result.joined_data_path.relative_to(static_root).as_posix()}?v={timestamp}"
        )
    return response


def _normalize_colname(value: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "", str(value).strip().lower().replace(" ", "_"))


def _read_table_preview(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path, nrows=5)
    return pd.read_csv(path, sep=None, engine="python", nrows=5)


def _find_column_case_insensitive(columns: list[str], expected: str) -> str | None:
    by_norm = {_normalize_colname(col): col for col in columns}
    return by_norm.get(_normalize_colname(expected))


def _validate_gwr_input_schema(
    table_path: Path,
    municipalities_path: Path,
    dependent_var: str,
    independent_vars: list[str],
    data_ibge_column: str | None,
    shape_ibge_column: str | None,
) -> tuple[list[str], str | None, str | None]:
    table_df = _read_table_preview(table_path)
    if table_df.empty:
        raise ValueError("Arquivo tabular enviado esta vazio.")

    table_cols = [str(col) for col in table_df.columns]
    resolved_dep = _find_column_case_insensitive(table_cols, dependent_var)
    if not resolved_dep:
        raise ValueError(f"Variavel dependente nao encontrada no tabular: {dependent_var}")

    resolved_indep: list[str] = []
    missing_indep: list[str] = []
    for var in independent_vars:
        hit = _find_column_case_insensitive(table_cols, var)
        if not hit:
            missing_indep.append(var)
        else:
            resolved_indep.append(hit)

    if missing_indep:
        missing_text = ", ".join(missing_indep)
        raise ValueError(f"Variaveis independentes ausentes no tabular: {missing_text}")

    table_ibge_candidates = {
        "codigo_ibge",
        "cod_ibge",
        "ibge",
        "code_muni",
        "cod_mun",
        "cd_mun",
        "geocodigo",
        "id_municipio",
    }
    muni_ibge_candidates = {
        "code_muni",
        "codigo_ibge",
        "cod_ibge",
        "cd_mun",
        "cod_mun",
        "geocodigo",
        "id",
    }

    resolved_table_ibge = None
    if data_ibge_column:
        resolved_table_ibge = _find_column_case_insensitive(table_cols, data_ibge_column)
        if not resolved_table_ibge:
            raise ValueError(f"Coluna IBGE do tabular nao encontrada: {data_ibge_column}")
    else:
        by_norm = {_normalize_colname(col): col for col in table_cols}
        for candidate in table_ibge_candidates:
            if candidate in by_norm:
                resolved_table_ibge = by_norm[candidate]
                break

    if resolved_table_ibge is None:
        raise ValueError("Nao foi possivel detectar coluna IBGE no arquivo tabular.")

    municipalities_df = gpd.read_file(municipalities_path, rows=5)
    muni_cols = [str(col) for col in municipalities_df.columns]
    resolved_shape_ibge = None
    if shape_ibge_column:
        resolved_shape_ibge = _find_column_case_insensitive(muni_cols, shape_ibge_column)
        if not resolved_shape_ibge:
            raise ValueError(f"Coluna IBGE do geoespacial nao encontrada: {shape_ibge_column}")
    else:
        by_norm = {_normalize_colname(col): col for col in muni_cols}
        for candidate in muni_ibge_candidates:
            if candidate in by_norm:
                resolved_shape_ibge = by_norm[candidate]
                break

    if resolved_shape_ibge is None:
        raise ValueError("Nao foi possivel detectar coluna IBGE no arquivo geoespacial.")

    return resolved_indep, resolved_table_ibge, resolved_shape_ibge


def _row_value(row: dict, keys: list[str]) -> str:
    for key in keys:
        for row_key, row_value in row.items():
            if _normalize_token(row_key) == key:
                return str(row_value or "").strip()
    return ""


def _parse_number(raw: str) -> float | None:
    text = str(raw or "").strip()
    if not text:
        return None
    if text in {"-", "--", "...", ".", ".."}:
        return None
    text = text.replace("%", "").replace(" ", "")
    text = text.replace(".", "").replace(",", ".") if text.count(",") == 1 and text.count(".") > 1 else text
    text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def _decode_text(raw_bytes: bytes) -> str:
    for encoding in ("utf-8-sig", "latin-1", "cp1252"):
        try:
            return raw_bytes.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw_bytes.decode("utf-8", errors="ignore")


def _find_header_index(lines: list[str]) -> int | None:
    for idx, line in enumerate(lines):
        if ";" not in line:
            continue
        normalized = _normalize_header(line)
        if "munic" in normalized and "total" in normalized:
            return idx
    return None


def _clean_municipio_name(raw_name: str) -> str:
    value = str(raw_name or "").strip().strip('"')
    value = re.sub(r"^\d{6}\s+", "", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _load_disease_csv(disease_key: str) -> tuple[dict[str, float], Path] | tuple[None, None]:
    csv_path = _resolve_disease_csv_path(disease_key)
    if not csv_path:
        return None, None

    municipality_keys = [
        "municipio",
        "municipio_nome",
        "nome_municipio",
        "cidade",
        "localidade",
        "name",
    ]
    value_keys = [
        "valor",
        "casos",
        "incidencia",
        "indice",
        "score",
        "taxa",
        "total",
        "total_",
        "value",
    ]

    values: dict[str, float] = {}
    raw_bytes = csv_path.read_bytes()
    content = _decode_text(raw_bytes)
    lines = [line for line in content.splitlines() if line.strip()]
    header_idx = _find_header_index(lines)
    if header_idx is None:
        return values, csv_path

    data_text = "\n".join(lines[header_idx:])
    reader = csv.reader(io.StringIO(data_text), delimiter=";")
    rows = list(reader)
    if not rows:
        return values, csv_path

    headers = [str(col).strip().strip('"') for col in rows[0]]
    normalized_headers = [_normalize_header(col) for col in headers]

    municipio_idx = None
    for idx, token in enumerate(normalized_headers):
        if any(key in token for key in municipality_keys) or "munic" in token:
            municipio_idx = idx
            break

    value_idx = None
    for idx, token in enumerate(normalized_headers):
        if token in value_keys or token.startswith("total"):
            value_idx = idx
            break

    if municipio_idx is None or value_idx is None:
        return values, csv_path

    for raw_row in rows[1:]:
        if len(raw_row) <= max(municipio_idx, value_idx):
            continue

        municipio = _clean_municipio_name(raw_row[municipio_idx])
        if not municipio:
            continue
        if _normalize_header(municipio) == "total":
            continue

        num = _parse_number(raw_row[value_idx])
        if num is None:
            continue

        values[municipio] = num

    return values, csv_path


def _find_municipio_disease_values(municipio_nome: str) -> dict[str, float]:
    target = _normalize_municipio_key(municipio_nome)
    result: dict[str, float] = {}

    for disease_key in DISEASE_CATALOG:
        values, _ = _load_disease_csv(disease_key)
        if not values:
            continue
        for municipio, value in values.items():
            if _normalize_municipio_key(municipio) == target:
                result[disease_key] = float(value)
                break

    return result


def _http_get_json(base_url: str, params: dict[str, str | float | int] | None = None, timeout: int = 10) -> dict | None:
    url = base_url
    if params:
        url = base_url + ("?" + urlencode(params))

    try:
        req = Request(url, headers={"User-Agent": "EpiGeoData/1.0"})
        with urlopen(req, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except (URLError, HTTPError, TimeoutError, json.JSONDecodeError):
        return None


def _fetch_realtime_environment(lat: float, lon: float) -> dict:
    climate = _http_get_json(
        "https://api.open-meteo.com/v1/forecast",
        {
            "latitude": lat,
            "longitude": lon,
            "current": "temperature_2m,precipitation",
            "timezone": "auto",
        },
    ) or {}

    topo = _http_get_json(
        "https://api.open-topo-data.org/v1/aster30m",
        {"locations": f"{lat},{lon}"},
    ) or {}

    current = climate.get("current", {}) if isinstance(climate, dict) else {}
    temperature = current.get("temperature_2m")
    precipitation = current.get("precipitation")

    elevation = None
    if isinstance(topo, dict):
        results = topo.get("results") or []
        if results and isinstance(results[0], dict):
            elevation = results[0].get("elevation")

    temp_val = float(temperature) if isinstance(temperature, (int, float)) else 27.0
    rain_val = float(precipitation) if isinstance(precipitation, (int, float)) else 0.0
    elev_val = float(elevation) if isinstance(elevation, (int, float)) else 280.0

    vegetation_idx = max(0.0, min(1.0, (rain_val / 15.0) * 0.55 + ((32.0 - temp_val) / 20.0) * 0.45))
    hydro_idx = max(0.0, min(1.0, (rain_val / 12.0) * 0.7 + (1.0 - min(elev_val / 1200.0, 1.0)) * 0.3))

    return {
        "pluviosidade_mm_h": round(rain_val, 3),
        "temperatura_c": round(temp_val, 2),
        "cobertura_vegetal_idx": round(vegetation_idx, 4),
        "relevo_elevacao_m": round(elev_val, 2),
        "hidrografia_proximidade_idx": round(hydro_idx, 4),
        "fontes": {
            "pluviosidade": "Open-Meteo",
            "temperatura": "Open-Meteo",
            "relevo": "OpenTopoData (ASTER30m)",
            "cobertura_vegetal": "Índice derivado de chuva+temperatura",
            "hidrografia": "Índice proxy derivado de chuva+relevo",
        },
        "timestamp_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }


@app.post("/api/realtime/municipio")
def get_realtime_municipio() -> tuple[dict, int]:
    payload = request.get_json(silent=True) or {}
    municipio_nome = str(payload.get("municipio", "")).strip()
    lat = payload.get("lat")
    lon = payload.get("lon")

    if not municipio_nome:
        return {"error": "Informe o município"}, 400
    if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
        return {"error": "Informe lat/lon numéricos"}, 400

    cache_key = f"{_normalize_municipio_key(municipio_nome)}:{round(float(lat), 3)}:{round(float(lon), 3)}"
    now_ts = datetime.utcnow().timestamp()
    cached = REALTIME_CACHE.get(cache_key)
    if cached and (now_ts - cached[0] <= REALTIME_CACHE_TTL_SECONDS):
        return jsonify(cached[1]), 200

    climate_env = _fetch_realtime_environment(float(lat), float(lon))
    datasus_local = _find_municipio_disease_values(municipio_nome)

    response = {
        "municipio": municipio_nome,
        "lat": lat,
        "lon": lon,
        "clima_ambiente": climate_env,
        "datasus": {
            "modo": "tempo real com fallback local",
            "fonte_primaria": "OpenDataSUS/DATASUS (quando configurado)",
            "fonte_fallback": "CSV local por agravo",
            "agravos": datasus_local,
        },
    }

    REALTIME_CACHE[cache_key] = (now_ts, response)
    return jsonify(response), 200


@app.get("/api/datasus/catalog")
def get_datasus_catalog() -> tuple[dict, int]:
    catalog: dict[str, dict] = {}

    for disease_key in DISEASE_CATALOG:
        payload = _build_disease_payload(disease_key)
        if payload is not None:
            catalog[disease_key] = payload

    return jsonify(
        {
            "fonte": "CATALOGO DATASUS com verificacao de disponibilidade ao vivo e fallback local por municipio",
            "agravos": catalog,
            "total_agravos": len(catalog),
        }
    ), 200


@app.get("/api/cartography/pernambuco")
def get_pernambuco_cartography() -> tuple[dict, int]:
    try:
        gdf, municipalities_geojson, state_geojson, catalog, source_path = _load_pernambuco_cartography()
    except FileNotFoundError as error:
        return {"error": str(error)}, 404

    return jsonify(
        {
            "ok": True,
            "source": str(source_path.relative_to(Path(__file__).parent)),
            "summary": {
                "total_municipios": int(len(gdf)),
                "crs": str(gdf.crs),
            },
            "state": state_geojson,
            "municipalities": municipalities_geojson,
            "catalog": catalog,
        }
    ), 200


@app.get("/api/cartography/pernambuco/municipios/<ibge_code>")
def get_pernambuco_municipality_geometry(ibge_code: str):
    try:
        gdf, _, _, _, _ = _load_pernambuco_cartography()
    except FileNotFoundError as error:
        return {"error": str(error)}, 404

    normalized_code = _normalize_ibge_code(ibge_code)
    selected = gdf[gdf["code_muni"] == normalized_code]
    if selected.empty:
        return {"error": "Municipio nao encontrado", "ibge_code": ibge_code}, 404

    return jsonify(json.loads(selected.to_json())), 200


def _check_password(password: str) -> bool:
    expected = os.getenv("ACCESS_PASSWORD", "epigeodata123")
    return password == expected


def _parse_payload() -> tuple[str, list[str], list[int], str, str, str, str, str, str, str]:
    payload = request.get_json(silent=True) or {}
    disease = payload.get("disease", "Nao informado")
    climates = payload.get("climates", [])
    analysis_years = payload.get("analysis_years", [])
    geres = payload.get("geres", "Todas as GERES")
    municipio = payload.get("municipio", "Todos os municipios")
    socio_variable = payload.get("sociodemographic_variable", "Nao informado")
    socio_scope = payload.get("sociodemographic_scope", "Nao informado")
    requester_name = payload.get("requester_name", "Nao informado")
    requester_email = payload.get("requester_email", "Nao informado")
    requester_role = payload.get("requester_role", "Nao informado")
    return (
        disease,
        climates,
        analysis_years,
        geres,
        municipio,
        socio_variable,
        socio_scope,
        requester_name,
        requester_email,
        requester_role,
    )


@app.get("/")
def index() -> str:
    return render_template("index.html")


@app.post("/api/auth")
def auth() -> tuple[dict, int]:
    payload = request.get_json(silent=True) or {}
    if _check_password(payload.get("password", "")):
        return {"ok": True}, 200
    return {"ok": False, "message": "Senha invalida"}, 403


@app.post("/api/export/pdf")
def export_pdf():
    payload = request.get_json(silent=True) or {}
    if not _check_password(payload.get("password", "")):
        return jsonify({"message": "Acesso negado"}), 403

    disease, climates, analysis_years, geres, municipio, socio_variable, socio_scope, requester_name, requester_email, requester_role = _parse_payload()
    climates_text = ", ".join(climates) if climates else "Nenhuma variavel selecionada"
    years_text = ", ".join(str(y) for y in analysis_years) if analysis_years else "Nenhum ano selecionado"

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    pdf.setTitle("Relatorio EpiGeoData")
    pdf.setFont("Helvetica-Bold", 16)
    pdf.drawString(50, 800, "Relatorio EpiGeoData")
    pdf.setFont("Helvetica", 11)
    pdf.drawString(50, 770, f"Data/Hora: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    pdf.drawString(50, 745, f"Doenca selecionada: {disease}")
    pdf.drawString(50, 720, f"Variaveis climaticas: {climates_text}")
    pdf.drawString(50, 695, f"Anos de analise: {years_text}")
    pdf.drawString(50, 670, f"GERES: {geres}")
    pdf.drawString(50, 645, f"Municipio: {municipio}")
    pdf.drawString(50, 620, f"Sociodemografico: {socio_variable} ({socio_scope})")
    pdf.drawString(50, 595, f"Solicitante: {requester_name} ({requester_role})")
    pdf.drawString(50, 570, f"Email para retorno: {requester_email}")
    pdf.drawString(50, 545, "Observacao: Documento protegido por autenticacao no portal.")
    pdf.showPage()
    pdf.save()

    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=True,
        download_name="relatorio_epigeodata.pdf",
        mimetype="application/pdf",
    )


@app.post("/api/export/spreadsheet")
def export_spreadsheet():
    payload = request.get_json(silent=True) or {}
    if not _check_password(payload.get("password", "")):
        return jsonify({"message": "Acesso negado"}), 403

    disease, climates, analysis_years, geres, municipio, socio_variable, socio_scope, requester_name, requester_email, requester_role = _parse_payload()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["campo", "valor"])
    writer.writerow(["data_utc", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")])
    writer.writerow(["doenca", disease])
    writer.writerow(["anos_analise", " | ".join(str(y) for y in analysis_years) if analysis_years else "nenhum"])
    writer.writerow(["geres", geres])
    writer.writerow(["municipio", municipio])
    writer.writerow(["variaveis_climaticas", " | ".join(climates) if climates else "nenhuma"])
    writer.writerow(["variavel_sociodemografica", socio_variable])
    writer.writerow(["escopo_sociodemografico", socio_scope])
    writer.writerow(["solicitante_nome", requester_name])
    writer.writerow(["solicitante_email", requester_email])
    writer.writerow(["solicitante_perfil", requester_role])

    content = io.BytesIO(output.getvalue().encode("utf-8"))
    return send_file(
        content,
        as_attachment=True,
        download_name="epigeodata_planilha.csv",
        mimetype="text/csv",
    )


@app.get("/api/climate-layers/<climate_type>")
def get_climate_layers(climate_type: str) -> tuple[dict, int]:
    """Retorna dados climáticos em GeoJSON"""
    valid_types = ["precipitacao", "temperatura", "queimadas", "cobertura_vegetal"]
    
    if climate_type not in valid_types:
        return {"error": f"Tipo climático inválido. Use: {', '.join(valid_types)}"}, 400
    
    data_file = Path(__file__).parent / f"data/climaticas/{climate_type}.geojson"
    
    if not data_file.exists():
        return {"error": f"Dados não disponíveis para {climate_type}"}, 404
    
    with open(data_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    return jsonify(data), 200


@app.get("/api/climate-layers")
def list_climate_layers() -> tuple[dict, int]:
    """Lista todos os tipos de camadas climáticas disponíveis e suas fontes vinculadas."""
    data_dir = Path(__file__).parent / "data/climaticas"

    available = []
    for climate_type, layer_name in CLIMATE_LAYER_BINDINGS.items():
        file_path = data_dir / f"{layer_name}.geojson"
        sources = []
        for source_name in CLIMATE_SOURCE_BINDINGS.get(climate_type, []):
            resolved = _resolve_climate_source_file(source_name)
            sources.append(
                {
                    "name": source_name,
                    "status": "disponivel" if resolved else "pendente",
                    "path": str(resolved.relative_to(Path(__file__).parent)) if resolved else None,
                }
            )

        item = {
            "tipo": climate_type,
            "layer": layer_name,
            "url": f"/api/climate-layers/{layer_name}",
            "status": "disponivel" if file_path.exists() else "indisponivel",
            "sources": sources,
        }
        available.append(item)

    return jsonify({"camadas": available, "total": len(available)}), 200


@app.get("/api/climate-sources")
def list_climate_sources() -> tuple[dict, int]:
    """Retorna o mapeamento de fontes climáticas para uso na plataforma."""
    data = []
    for climate_type, source_names in CLIMATE_SOURCE_BINDINGS.items():
        for source_name in source_names:
            resolved = _resolve_climate_source_file(source_name)
            data.append(
                {
                    "tipo": climate_type,
                    "source": source_name,
                    "status": "disponivel" if resolved else "pendente",
                    "path": str(resolved.relative_to(Path(__file__).parent)) if resolved else None,
                }
            )

    return jsonify({"sources": data, "total": len(data)}), 200


@app.get("/api/disease-data/<disease_key>")
def get_disease_data(disease_key: str) -> tuple[dict, int]:
    """Retorna dados do agravo com metadados DATASUS e fallback local por municipio."""
    payload = _build_disease_payload(disease_key)
    if payload is None:
        expected = ", ".join(sorted(DISEASE_CATALOG.keys()))
        return {
            "error": "Agravo nao encontrado no catalogo",
            "disease": disease_key,
            "expected_keys": expected,
        }, 404

    return jsonify(payload), 200


@app.post("/api/maps/professional-overlay")
def generate_professional_overlay_map() -> tuple[dict, int]:
    payload = request.get_json(silent=True) or {}
    disease_key = str(payload.get("disease_key", "tuberculose")).strip()
    title = str(payload.get("title", "")).strip() or DEFAULT_PROFESSIONAL_MAP_TITLE

    try:
        from scripts.generate_choropleth_brazil import generate_professional_choropleth

        result = generate_professional_choropleth(
            disease_key=disease_key,
            title=title,
        )
    except FileNotFoundError as error:
        return {"error": str(error)}, 404
    except Exception as error:  # pragma: no cover - fallback operacional
        return {
            "error": "Falha ao gerar mapa profissional",
            "details": str(error),
        }, 500

    static_root = Path(__file__).parent / "static"
    relative = result.output_file.relative_to(static_root)
    image_url = f"/static/{relative.as_posix()}?v={int(datetime.utcnow().timestamp())}"

    return jsonify(
        {
            "ok": True,
            "disease_key": result.disease_key,
            "image_url": image_url,
            "source_csv": (
                str(result.source_csv.relative_to(Path(__file__).parent))
                if result.source_csv is not None
                else None
            ),
            "variable": result.variable_label,
            "has_local_data": result.has_local_data,
        }
    ), 200


@app.route("/download")
def download():
    target = Path(__file__).parent / "mapa_ibge_style.png"

    if not target.exists():
        try:
            from scripts.generate_choropleth_brazil import generate_professional_choropleth

            generated = generate_professional_choropleth(
                disease_key="scz",
                title="Sindrome Congenita da Zika",
                output_filename="mapa_ibge_style.png",
                dpi=300,
            )
            target = generated.output_file
        except Exception as error:  # pragma: no cover - rota operacional
            return jsonify({"error": "Falha ao gerar mapa para download", "details": str(error)}), 500

    return send_file(target, as_attachment=True)


@app.post("/api/maps/prepared-heatmap-overlay")
def generate_prepared_heatmap_overlay() -> tuple[dict, int]:
    payload = request.get_json(silent=True) or {}
    requested_file = str(payload.get("input_file", DEFAULT_PREPARED_HEATMAP_FILE)).strip()
    prepared_file = _resolve_prepared_heatmap_file(requested_file)

    if not prepared_file:
        return {
            "error": "Arquivo preparado nao encontrado",
            "requested": requested_file,
            "expected_examples": [
                "municpios_pe",
                "municpios_pe.csv",
                "municipios_pe.csv",
                "municipios_pe.xlsx",
            ],
        }, 404

    prefix = f"overlay_preparado_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

    try:
        from scripts.generate_pernambuco_heatmap import generate_pernambuco_heatmaps

        outputs = generate_pernambuco_heatmaps(
            input_path=prepared_file,
            output_dir=Path(__file__).parent / "static" / "maps",
            prefix=prefix,
            dpi=300,
        )
    except Exception as error:  # pragma: no cover - erro de runtime em ambiente
        return {
            "error": "Falha ao gerar mapas da sobreposicao preparada",
            "details": str(error),
            "input_file": str(prepared_file.relative_to(Path(__file__).parent)),
        }, 500

    static_root = Path(__file__).parent / "static"
    ts = int(datetime.utcnow().timestamp())

    def _to_static_url(path: Path) -> str:
        relative = path.relative_to(static_root)
        return f"/static/{relative.as_posix()}?v={ts}"

    return jsonify(
        {
            "ok": True,
            "input_file": str(prepared_file.relative_to(Path(__file__).parent)),
            "maps": {
                "base": _to_static_url(outputs.base_map),
                "marked": _to_static_url(outputs.marked_map),
                "combined": _to_static_url(outputs.combined_map),
            },
        }
    ), 200


@app.post("/api/maps/epidemiological-gwr")
def generate_epidemiological_gwr_maps_api() -> tuple[dict, int]:
    payload = request.get_json(silent=True) or {}

    table_path_raw = str(payload.get("table_path", "")).strip()
    municipalities_path_raw = str(payload.get("municipalities_path", "")).strip()
    dependent_var = str(payload.get("dependent_var", "")).strip()
    independent_vars = payload.get("independent_vars", [])

    if not table_path_raw or not municipalities_path_raw or not dependent_var:
        return {
            "error": "Campos obrigatorios ausentes",
            "required": ["table_path", "municipalities_path", "dependent_var", "independent_vars"],
        }, 400

    if not isinstance(independent_vars, list) or not independent_vars:
        return {"error": "'independent_vars' deve ser uma lista nao vazia"}, 400

    table_path = _resolve_workspace_file(table_path_raw)
    municipalities_path = _resolve_workspace_file(municipalities_path_raw)

    if not table_path:
        return {"error": "Arquivo tabular nao encontrado", "table_path": table_path_raw}, 404
    if not municipalities_path:
        return {"error": "Arquivo geoespacial nao encontrado", "municipalities_path": municipalities_path_raw}, 404

    output_dir = Path(__file__).parent / "static" / "maps"

    try:
        from scripts.generate_epidemiological_gwr_maps import generate_epidemiological_gwr_maps

        result = generate_epidemiological_gwr_maps(
            tabular_data_path=table_path,
            municipalities_path=municipalities_path,
            dependent_var=dependent_var,
            independent_vars=[str(v) for v in independent_vars],
            output_dir=output_dir,
            data_ibge_column=(str(payload.get("data_ibge_column")).strip() or None),
            shape_ibge_column=(str(payload.get("shape_ibge_column")).strip() or None),
            classification_scheme=str(payload.get("classification_scheme", "natural_breaks")).strip(),
            n_classes=int(payload.get("n_classes", 5)),
            target_crs=str(payload.get("target_crs", "EPSG:31985")).strip(),
            dpi=int(payload.get("dpi", 300)),
            title_prefix=str(payload.get("title_prefix", "Pernambuco - Analise Espacial Epidemiologica")).strip(),
        )
    except ValueError as error:
        return {"error": str(error)}, 400
    except Exception as error:  # pragma: no cover - erro em runtime
        return {
            "error": "Falha ao executar pipeline epidemiologica GWR",
            "details": str(error),
        }, 500

    static_root = Path(__file__).parent / "static"
    return jsonify(_build_gwr_api_response(result, static_root)), 200


@app.post("/api/maps/epidemiological-gwr-upload")
def generate_epidemiological_gwr_maps_upload_api() -> tuple[dict, int]:
    table_file = request.files.get("table_file")
    municipalities_file = request.files.get("municipalities_file")

    dependent_var = str(request.form.get("dependent_var", "")).strip()
    independent_vars = _parse_independent_vars_form(request.form)

    if not table_file or not municipalities_file or not dependent_var:
        return {
            "error": "Campos obrigatorios ausentes",
            "required": ["table_file", "municipalities_file", "dependent_var", "independent_vars"],
        }, 400

    if not independent_vars:
        return {"error": "'independent_vars' deve ser informado no form-data"}, 400

    run_stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    uploads_root = Path(__file__).parent / "data" / "uploads" / run_stamp
    uploads_root.mkdir(parents=True, exist_ok=True)

    table_path = _save_uploaded_file(table_file, uploads_root, "tabular")
    municipalities_raw_path = _save_uploaded_file(municipalities_file, uploads_root, "municipios")

    try:
        municipalities_path = _resolve_uploaded_municipalities_file(
            municipalities_raw_path,
            uploads_root / "municipios_extraidos",
        )
    except ValueError as error:
        return {"error": str(error)}, 400

    data_ibge_form = (str(request.form.get("data_ibge_column", "")).strip() or None)
    shape_ibge_form = (str(request.form.get("shape_ibge_column", "")).strip() or None)

    try:
        resolved_independent_vars, resolved_data_ibge_col, resolved_shape_ibge_col = _validate_gwr_input_schema(
            table_path=table_path,
            municipalities_path=municipalities_path,
            dependent_var=dependent_var,
            independent_vars=independent_vars,
            data_ibge_column=data_ibge_form,
            shape_ibge_column=shape_ibge_form,
        )
    except ValueError as error:
        return {"error": str(error)}, 400

    output_dir = Path(__file__).parent / "static" / "maps"

    try:
        from scripts.generate_epidemiological_gwr_maps import generate_epidemiological_gwr_maps

        result = generate_epidemiological_gwr_maps(
            tabular_data_path=table_path,
            municipalities_path=municipalities_path,
            dependent_var=dependent_var,
            independent_vars=resolved_independent_vars,
            output_dir=output_dir,
            data_ibge_column=resolved_data_ibge_col,
            shape_ibge_column=resolved_shape_ibge_col,
            classification_scheme=str(request.form.get("classification_scheme", "natural_breaks")).strip(),
            n_classes=int(request.form.get("n_classes", "5")),
            target_crs=str(request.form.get("target_crs", "EPSG:31985")).strip(),
            dpi=int(request.form.get("dpi", "300")),
            title_prefix=str(request.form.get("title_prefix", "Pernambuco - Analise Espacial Epidemiologica")).strip(),
        )
    except ValueError as error:
        return {"error": str(error)}, 400
    except Exception as error:  # pragma: no cover - erro em runtime
        return {
            "error": "Falha ao executar pipeline epidemiologica GWR via upload",
            "details": str(error),
        }, 500

    static_root = Path(__file__).parent / "static"
    response = _build_gwr_api_response(result, static_root)
    response["uploaded_inputs"] = {
        "table_file": str(table_path.relative_to(Path(__file__).parent)),
        "municipalities_file": str(municipalities_path.relative_to(Path(__file__).parent)),
    }

    return jsonify(response), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

