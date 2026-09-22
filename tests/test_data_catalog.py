import json
import tempfile
import unittest
from pathlib import Path

from data_catalog import catalogue, assess_analysis


class DataCatalogTests(unittest.TestCase):
    def test_missing_sources_are_reported(self):
        with tempfile.TemporaryDirectory() as directory:
            items = catalogue(Path(directory))
            self.assertTrue(all(not item['available'] for item in items))
            result = assess_analysis('Esquistossomose', 'V GERES', 'Todos', 'Temperatura', 'GWR', Path(directory))
            self.assertFalse(result['can_run_gwr'])
            self.assertIsNone(result['results'])
            self.assertTrue(any('Fonte obrigatória ausente' in item for item in result['issues']))

    def test_climate_observations_are_counted_without_imputation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'data/climaticas').mkdir(parents=True)
            (root / 'data/climaticas/temperatura.geojson').write_text(json.dumps({'features': [{'properties': {'municipio_nome': 'Recife', 'data': '2024-03-23'}}]}), encoding='utf-8')
            item = next(entry for entry in catalogue(root) if entry['id'] == 'temperatura_pontos')
            self.assertEqual(item['observations'], 1)
            self.assertEqual(item['municipalities_observed'], ['Recife'])
            self.assertFalse(assess_analysis('Esquistossomose', 'V GERES', 'Todos', 'Temperatura', 'GWR', root)['can_run_gwr'])

    def test_blueprint_validation_and_blocking(self):
        from flask import Flask
        from catalog_routes import catalog_blueprint
        app = Flask(__name__)
        app.register_blueprint(catalog_blueprint)
        client = app.test_client()
        self.assertEqual(client.post('/api/catalog/assess', json={}).status_code, 400)
        response = client.post('/api/catalog/assess', json={'disease': 'Esquistossomose', 'territory': 'V GERES', 'sex': 'Todos', 'climate': 'Temperatura', 'method': 'GWR'})
        self.assertEqual(response.status_code, 422)
        self.assertFalse(response.get_json()['can_run_gwr'])


if __name__ == '__main__':
    unittest.main()
