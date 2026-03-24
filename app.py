import csv
import io
import json
import os
import re
from pathlib import Path
from datetime import datetime

from flask import Flask, jsonify, render_template, request, send_file
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

app = Flask(__name__)


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
    text = text.replace("%", "").replace(" ", "")
    text = text.replace(".", "").replace(",", ".") if text.count(",") == 1 and text.count(".") > 1 else text
    text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


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
        "value",
    ]

    values: dict[str, float] = {}
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            municipio = _row_value(row, municipality_keys)
            if not municipio:
                continue
            raw = _row_value(row, value_keys)
            num = _parse_number(raw)
            if num is None:
                continue
            values[municipio] = num

    return values, csv_path


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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

