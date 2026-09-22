"""Regression tests for the direct legacy generator entrypoint."""
import tempfile
import unittest
from pathlib import Path
from scripts.generate_epidemiological_gwr_maps import (
    _normalize_ibge_code, generate_epidemiological_gwr_maps,
)
from stage3_validation import DataValidationError
from stage3_pipeline_gate import ModelNotAuthorized


class GWRFailClosedTests(unittest.TestCase):
    def test_ibge_seven_digits_preserved(self):
        self.assertEqual(_normalize_ibge_code('2604106.0'), '2604106')
        with self.assertRaises(DataValidationError):
            _normalize_ibge_code('260410')

    def test_explicit_year_required_before_any_output(self):
        with tempfile.TemporaryDirectory() as root:
            out = Path(root) / 'maps'
            with self.assertRaises(DataValidationError):
                generate_epidemiological_gwr_maps('missing.csv', 'missing.geojson', 'positividade', ['chuva'], out)
            self.assertFalse(out.exists())

    def test_direct_generator_cannot_bypass_authorization(self):
        with tempfile.TemporaryDirectory() as root:
            out = Path(root) / 'maps'
            with self.assertRaises(ModelNotAuthorized):
                generate_epidemiological_gwr_maps('missing.csv', 'missing.geojson', 'positividade', ['chuva'], out, analysis_year=2021)
            self.assertFalse(out.exists())

    def test_duplicate_predictors_rejected(self):
        with self.assertRaises(DataValidationError):
            generate_epidemiological_gwr_maps('missing.csv', 'missing.geojson', 'positividade', ['chuva', 'chuva'], analysis_year=2021)


if __name__ == '__main__':
    unittest.main()
