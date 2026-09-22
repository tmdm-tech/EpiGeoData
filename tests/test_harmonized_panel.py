import tempfile
import unittest
from pathlib import Path

from harmonized_panel import HarmonizationError, export_panel, harmonize


PROVENANCE = {"organization": "Verified institution", "dataset": "Observed dataset", "version": "2026-01", "retrieved_at": "2026-09-22", "reference_url": "https://example.org/source", "method": "documented", "verified": True}


def fixtures():
    territory = [{"municipio_ibge": "2604106", "geres": "V", "provenance": dict(PROVENANCE)}]
    climate = [{"municipio_ibge": "2604106", "ano": 2021, "temperatura_c": 28, "provenance": dict(PROVENANCE)}]
    epidemiology = [{"municipio_ibge": "2604106", "ano": 2021, "estrato": "todos", "positividade_percentual": 1.5, "provenance": dict(PROVENANCE)}]
    return epidemiology, climate, territory


class HarmonizationTests(unittest.TestCase):
    def build(self, epi, climate, territory):
        return harmonize(epi, climate, territory, expected_years=[2021], expected_municipalities=["2604106"])

    def test_preserves_key_and_provenance(self):
        result = self.build(*fixtures())
        self.assertEqual(len(result), 1)
        self.assertEqual((result[0]["municipio_ibge"], result[0]["ano"], result[0]["estrato"]), ("2604106", 2021, "todos"))
        self.assertEqual(result[0]["climate_provenance"]["dataset"], "Observed dataset")

    def test_rejects_six_digit_code(self):
        epi, climate, territory = fixtures()
        epi[0]["municipio_ibge"] = "260410"
        with self.assertRaises(HarmonizationError): self.build(epi, climate, territory)

    def test_rejects_missing_year(self):
        epi, climate, territory = fixtures()
        climate[0]["ano"] = ""
        with self.assertRaises(HarmonizationError): self.build(epi, climate, territory)

    def test_rejects_mismatched_period(self):
        epi, climate, territory = fixtures()
        climate[0]["ano"] = 2024
        with self.assertRaises(HarmonizationError): self.build(epi, climate, territory)

    def test_rejects_duplicate_epi_key(self):
        epi, climate, territory = fixtures()
        epi.append(dict(epi[0]))
        with self.assertRaises(HarmonizationError): self.build(epi, climate, territory)

    def test_rejects_duplicate_climate_key(self):
        epi, climate, territory = fixtures()
        climate.append(dict(climate[0]))
        with self.assertRaises(HarmonizationError): self.build(epi, climate, territory)

    def test_rejects_missing_territory(self):
        epi, climate, territory = fixtures()
        with self.assertRaises(HarmonizationError): self.build(epi, climate, [])

    def test_rejects_unverified_source(self):
        epi, climate, territory = fixtures()
        climate[0]["provenance"]["verified"] = False
        with self.assertRaises(HarmonizationError): self.build(epi, climate, territory)

    def test_rejects_missing_stratum(self):
        epi, climate, territory = fixtures()
        epi[0]["estrato"] = ""
        with self.assertRaises(HarmonizationError): self.build(epi, climate, territory)

    def test_export_does_not_overwrite(self):
        rows = self.build(*fixtures())
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "panel.csv"
            export_panel(rows, path)
            self.assertIn("2604106", path.read_text(encoding="utf-8"))
            with self.assertRaises(HarmonizationError): export_panel(rows, path)


if __name__ == "__main__": unittest.main()
