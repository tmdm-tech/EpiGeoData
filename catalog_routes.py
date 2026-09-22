"""Register with app.register_blueprint(catalog_blueprint) during Flask app setup."""
from flask import Blueprint, jsonify, request
from data_catalog import catalogue, assess_analysis

catalog_blueprint = Blueprint("data_catalog", __name__)

@catalog_blueprint.get("/api/catalog/datasets")
def list_datasets():
    return jsonify({"datasets": catalogue()}), 200

@catalog_blueprint.post("/api/catalog/assess")
def assess():
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"error": "JSON object required"}), 400
    fields = ("disease", "territory", "sex", "climate", "method")
    if any(not isinstance(payload.get(key), str) or not payload[key].strip() for key in fields):
        return jsonify({"error": "Required string fields", "fields": list(fields)}), 400
    result = assess_analysis(*(payload[key].strip() for key in fields))
    return jsonify(result), 422 if result["status"] == "blocked" else 200
