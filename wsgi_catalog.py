"""Development WSGI entrypoint: preserves the existing Flask application and adds catalog routes.

Run locally with: gunicorn wsgi_catalog:app
The Render production start command remains unchanged until the integration is reviewed.
"""
from app import app
from catalog_routes import catalog_blueprint

if catalog_blueprint.name not in app.blueprints:
    app.register_blueprint(catalog_blueprint)
