import csv
import io
import json
import os
import re
from pathlib import Path
from datetime import datetime
from urllib.parse import urlencode
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

from flask import Flask, jsonify, render_template, request, send_file
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

app = Flask(__name__)

REALTIME_CACHE: dict[str, tuple[float, dict]] = {}
REALTIME_CACHE_TTL_SECONDS = 20 * 60
DEFAULT_PROFESSIONAL_MAP_TITLE = "EpiGeoData | Mapa Coropletico Cientifico - Pernambuco"
DEFAULT_PREPARED_HEATMAP_FILE = "municpios_pe"


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


def _normalize_header(value: str) -> str:
    value = str(value or "").strip().lower()
    value = re.sub(r"\s+", "_", value)
    value = re.sub(r"[^a-z0-9_]+", "", value)
    return value


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
    token = _normalize_token(disease_key)
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

    for disease_key in DISEASE_FILE_ALIASES:
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

    for disease_key in DISEASE_FILE_ALIASES:
        values, path = _load_disease_csv(disease_key)
        nums = list(values.values()) if values else []
        catalog[disease_key] = {
            "source": str(path.relative_to(Path(__file__).parent)) if path else None,
            "total_municipios": len(nums),
            "min": min(nums) if nums else None,
            "max": max(nums) if nums else None,
            "mean": round(sum(nums) / len(nums), 4) if nums else None,
            "municipios": values or {},
        }

    return jsonify(
        {
            "fonte": "CATALOGO DATASUS local (CSV) com atualização em tempo real na visualização",
            "agravos": catalog,
            "total_agravos": len(catalog),
        }
    ), 200


def _check_password(password: str) -> bool:
    expected = os.getenv("ACCESS_PASSWORD", "epigeodata123")
    return password == expected


def _parse_payload() -> tuple[str, list[str], str, str, str, str, str, str, str]:
    payload = request.get_json(silent=True) or {}
    disease = payload.get("disease", "Nao informado")
    climates = payload.get("climates", [])
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

    disease, climates, geres, municipio, socio_variable, socio_scope, requester_name, requester_email, requester_role = _parse_payload()
    climates_text = ", ".join(climates) if climates else "Nenhuma variavel selecionada"

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    pdf.setTitle("Relatorio EpiGeoData")
    pdf.setFont("Helvetica-Bold", 16)
    pdf.drawString(50, 800, "Relatorio EpiGeoData")
    pdf.setFont("Helvetica", 11)
    pdf.drawString(50, 770, f"Data/Hora: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    pdf.drawString(50, 745, f"Doenca selecionada: {disease}")
    pdf.drawString(50, 720, f"Variaveis climaticas: {climates_text}")
    pdf.drawString(50, 695, f"GERES: {geres}")
    pdf.drawString(50, 670, f"Municipio: {municipio}")
    pdf.drawString(50, 645, f"Sociodemografico: {socio_variable} ({socio_scope})")
    pdf.drawString(50, 620, f"Solicitante: {requester_name} ({requester_role})")
    pdf.drawString(50, 595, f"Email para retorno: {requester_email}")
    pdf.drawString(50, 570, "Observacao: Documento protegido por autenticacao no portal.")
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

    disease, climates, geres, municipio, socio_variable, socio_scope, requester_name, requester_email, requester_role = _parse_payload()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["campo", "valor"])
    writer.writerow(["data_utc", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")])
    writer.writerow(["doenca", disease])
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
    """Lista todos os tipos de camadas climáticas disponíveis"""
    climate_types = ["precipitacao", "temperatura", "queimadas", "cobertura_vegetal"]
    data_dir = Path(__file__).parent / "data/climaticas"
    
    available = []
    for climate_type in climate_types:
        file_path = data_dir / f"{climate_type}.geojson"
        if file_path.exists():
            available.append({
                "tipo": climate_type,
                "url": f"/api/climate-layers/{climate_type}",
                "status": "disponível"
            })
    
    return jsonify({
        "camadas": available,
        "total": len(available)
    }), 200


@app.get("/api/disease-data/<disease_key>")
def get_disease_data(disease_key: str) -> tuple[dict, int]:
    """Retorna dados de doença carregados de CSV local."""
    values, path = _load_disease_csv(disease_key)
    if values is None or path is None:
        expected = ", ".join(sorted(DISEASE_FILE_ALIASES.keys()))
        return {
            "error": "Arquivo CSV de doença não encontrado",
            "disease": disease_key,
            "expected_keys": expected,
        }, 404

    nums = list(values.values())
    return jsonify(
        {
            "disease": disease_key,
            "source": str(path.relative_to(Path(__file__).parent)),
            "municipios": values,
            "summary": {
                "total_municipios": len(nums),
                "min": min(nums) if nums else None,
                "max": max(nums) if nums else None,
                "mean": round(sum(nums) / len(nums), 4) if nums else None,
            },
        }
    ), 200


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
            "source_csv": str(result.source_csv.relative_to(Path(__file__).parent)),
            "variable": result.variable_label,
        }
    ), 200


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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

