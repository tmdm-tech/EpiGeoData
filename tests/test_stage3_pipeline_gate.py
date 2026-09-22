import tempfile
import unittest
from pathlib import Path

from stage3_pipeline_gate import ModelNotAuthorized, require_authorized_gwr


class PipelineGateTests(unittest.TestCase):
    def test_missing_sources_fail_closed(self):
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaises(ModelNotAuthorized):
                require_authorized_gwr(Path(folder) / 'missing.csv', Path(folder) / 'missing.geojson', 'positividade_percentual', ['temperatura'])

    def test_valid_looking_panel_still_cannot_authorize_model(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            table = root / 'panel.csv'
            table.write_text('codigo_ibge,ano,positividade_percentual,temperatura\n2604106,2021,1.2,28\n', encoding='utf-8')
            geometry = root / 'municipios.geojson'
            geometry.write_text('{"type":"FeatureCollection","features":[]}', encoding='utf-8')
            with self.assertRaisesRegex(ModelNotAuthorized, 'not homologated'):
                require_authorized_gwr(table, geometry, 'positividade_percentual', ['temperatura'])

    def test_invalid_panel_is_rejected_before_modelling(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            table = root / 'panel.csv'
            table.write_text('codigo_ibge,ano,positividade_percentual\n260410,2021,1.2\n', encoding='utf-8')
            geometry = root / 'municipios.geojson'
            geometry.write_text('{}', encoding='utf-8')
            with self.assertRaisesRegex(ModelNotAuthorized, 'Panel validation failed'):
                require_authorized_gwr(table, geometry, 'positividade_percentual', ['temperatura'])


if __name__ == '__main__':
    unittest.main()
