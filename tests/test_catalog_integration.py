"""Integration tests for the real Flask app plus catalog WSGI entrypoint."""
import unittest
from wsgi_catalog import app


class CatalogIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()

    def test_catalog_route_is_registered(self):
        response = self.client.get('/api/catalog/datasets')
        self.assertEqual(response.status_code, 200)
        self.assertIn('datasets', response.get_json())

    def test_gwr_assessment_blocks_missing_aligned_data(self):
        response = self.client.post('/api/catalog/assess', json={
            'disease': 'Esquistossomose', 'territory': 'V GERES',
            'sex': 'Todos', 'climate': 'Temperatura', 'method': 'GWR',
        })
        self.assertEqual(response.status_code, 422)
        self.assertFalse(response.get_json()['can_run_gwr'])
        self.assertIsNone(response.get_json()['results'])

    def test_existing_app_routes_are_preserved(self):
        self.assertIn('data_catalog', app.blueprints)
        self.assertIn('static', app.view_functions)


if __name__ == '__main__':
    unittest.main()
