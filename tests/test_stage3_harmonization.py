import unittest
from stage3_harmonization import validate_spatial_keys
from stage3_validation import DataValidationError


class SpatialHarmonizationTests(unittest.TestCase):
    def setUp(self):
        self.geometry = [{'codigo_ibge': '2604106'}, {'codigo_ibge': '2611101'}]
        self.panel = [
            {'codigo_ibge': '2604106', 'ano': '2021'},
            {'codigo_ibge': '2604106', 'ano': '2022'},
            {'codigo_ibge': '2611101', 'ano': '2021'},
        ]

    def test_preserves_years_and_record_count(self):
        result = validate_spatial_keys(self.panel, self.geometry)
        self.assertEqual(result['panel_records'], 3)
        self.assertEqual(result['years'], ['2021', '2022'])
        self.assertFalse(result['model_authorized'])

    def test_missing_geometry_is_error(self):
        with self.assertRaises(DataValidationError):
            validate_spatial_keys(self.panel, self.geometry[:1])

    def test_missing_panel_is_error(self):
        with self.assertRaises(DataValidationError):
            validate_spatial_keys(self.panel[:2], self.geometry)

    def test_duplicate_geometry_is_error(self):
        with self.assertRaises(DataValidationError):
            validate_spatial_keys(self.panel, self.geometry + self.geometry[:1])

    def test_duplicate_panel_grain_is_error(self):
        with self.assertRaises(DataValidationError):
            validate_spatial_keys(self.panel + self.panel[:1], self.geometry)

    def test_six_digit_code_is_not_guessed(self):
        with self.assertRaises(DataValidationError):
            validate_spatial_keys([{'codigo_ibge': '260410', 'ano': '2021'}], self.geometry)


if __name__ == '__main__':
    unittest.main()
