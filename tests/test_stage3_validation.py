import unittest

from stage3_validation import DataValidationError, normalize_ibge, validate_municipal_panel


class Stage3ValidationTests(unittest.TestCase):
    def test_preserves_seven_digit_ibge(self):
        self.assertEqual(normalize_ibge('2604106.0'), '2604106')
        with self.assertRaises(DataValidationError):
            normalize_ibge('260410')

    def test_preserves_distinct_years(self):
        rows = [
            {'codigo_ibge': '2604106', 'ano': '2021', 'positividade_percentual': '1,2'},
            {'codigo_ibge': '2604106', 'ano': '2022', 'positividade_percentual': '2,4'},
        ]
        result = validate_municipal_panel(rows)
        self.assertEqual(result['records'], 2)
        self.assertEqual(result['years'], [2021, 2022])
        self.assertFalse(result['model_authorized'])

    def test_duplicate_municipality_year_rejected(self):
        row = {'codigo_ibge': '2604106', 'ano': '2021', 'positividade_percentual': '1.2'}
        with self.assertRaises(DataValidationError):
            validate_municipal_panel([row, row.copy()])

    def test_missing_outcome_not_imputed(self):
        with self.assertRaises(DataValidationError):
            validate_municipal_panel([{'codigo_ibge': '2604106', 'ano': '2021', 'positividade_percentual': '-'}])

    def test_sex_strata_are_distinct(self):
        rows = [
            {'codigo_ibge': '2604106', 'ano': '2021', 'positividade_percentual': '1.2', 'sexo': 'F'},
            {'codigo_ibge': '2604106', 'ano': '2021', 'positividade_percentual': '2.1', 'sexo': 'M'},
        ]
        self.assertEqual(validate_municipal_panel(rows, sex_key='sexo')['records'], 2)


if __name__ == '__main__':
    unittest.main()
