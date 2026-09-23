"""WSGI entrypoint for the existing Flask app with fail-closed catalog safeguards.

The legacy GWR pipeline does not preserve municipality/year/sex grain and cannot
be used for scientific results until harmonization and source provenance pass.
"""
from flask import jsonify, request

from app import app
from catalog_routes import catalog_blueprint

if catalog_blueprint.name not in app.blueprints:
    app.register_blueprint(catalog_blueprint)


@app.before_request
def block_unvalidated_gwr():
    """Prevent legacy GWR endpoints from bypassing the stage-3 validation gate."""
    if request.method == "POST" and request.path in (
        "/api/maps/epidemiological-gwr",
        "/api/maps/epidemiological-gwr-upload",
    ):
        return jsonify({
            "status": "blocked",
            "error": "GWR is not authorized for unvalidated input data",
            "issues": [
                "Municipality/year/sex grain and seven-digit IBGE codes must be preserved.",
                "GERES membership is synchronized with SES-PE and municipal names with IBGE; model-level territorial linkage and source provenance must still be validated.",
                "Epidemiological and climatic observations must align by municipality and period.",
                "Bandwidth, collinearity and residual diagnostics must be validated.",
            ],
            "results": None,
        }), 422
