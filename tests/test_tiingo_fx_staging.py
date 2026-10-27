import csv
import json
import tempfile
import unittest
from pathlib import Path

from src.staging.tiingo_fx_history import (
    STAGING_FIELDS,
    prepare_staging_file,
    validate_staging_file,
)


class TestTiingoFxStaging(unittest.TestCase):
    def test_prepares_raw_records_as_canonical_csv(self):
        records = [
            {
                "date": "2026-10-27T00:00:00.000Z",
                "ticker": "eurusd",
                "open": 1.11736,
                "high": 1.117695,
                "low": 1.11627,
                "close": 1.11713,
            },
        ]
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            raw_file = root / "raw.json"
            raw_file.write_text(json.dumps(records), encoding="utf-8")

            output_path = prepare_staging_file(raw_file, root / "staging")

            with output_path.open(encoding="utf-8", newline="") as staging_file:
                reader = csv.DictReader(staging_file)
                self.assertEqual(tuple(reader.fieldnames), STAGING_FIELDS)
                rows = list(reader)

            self.assertEqual(
                rows,
                [
                    {
                        "source": "tiingo",
                        "symbol": "EURUSD",
                        "event_time": "2026-10-27T00:00:00+00:00",
                        "open": "1.11736",
                        "high": "1.117695",
                        "low": "1.11627",
                        "close": "1.11713",
                    },
                ],
            )
            self.assertEqual(json.loads(raw_file.read_text(encoding="utf-8")), records)

    def test_rejects_missing_required_field(self):
        records = [{"date": "2026-10-27T00:00:00Z", "ticker": "eurusd"}]
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            raw_file = root / "raw.json"
            raw_file.write_text(json.dumps(records), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "campos obrigatórios ausentes"):
                prepare_staging_file(raw_file, root / "staging")

            self.assertFalse((root / "staging").exists())

    def test_rejects_invalid_price(self):
        records = [
            {
                "date": "2026-10-27T00:00:00Z",
                "ticker": "eurusd",
                "open": 1.1,
                "high": float("nan"),
                "low": 1.0,
                "close": 1.05,
            },
        ]
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            raw_file = root / "raw.json"
            raw_file.write_text(json.dumps(records), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "campo high"):
                prepare_staging_file(raw_file, root / "staging")

            self.assertFalse((root / "staging").exists())

    def test_validates_prepared_ohlc_rows(self):
        csv_content = (
            "source,symbol,event_time,open,high,low,close\n"
            "tiingo,EURUSD,2026-10-27T00:00:00+00:00,"
            "1.1,1.2,1.0,1.15\n"
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            staging_file = Path(temporary_directory) / "staging.csv"
            staging_file.write_text(csv_content, encoding="utf-8")

            self.assertEqual(validate_staging_file(staging_file), 1)

    def test_rejects_duplicate_symbol_and_date(self):
        row = "tiingo,EURUSD,2026-10-27T00:00:00+00:00,1.1,1.2,1.0,1.15"
        csv_content = (
            "source,symbol,event_time,open,high,low,close\n"
            f"{row}\n{row}\n"
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            staging_file = Path(temporary_directory) / "staging.csv"
            staging_file.write_text(csv_content, encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "duplicado"):
                validate_staging_file(staging_file)

    def test_rejects_inconsistent_ohlc_values(self):
        csv_content = (
            "source,symbol,event_time,open,high,low,close\n"
            "tiingo,EURUSD,2026-10-27T00:00:00+00:00,"
            "1.1,1.05,1.0,1.15\n"
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            staging_file = Path(temporary_directory) / "staging.csv"
            staging_file.write_text(csv_content, encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "OHLC são incoerentes"):
                validate_staging_file(staging_file)


if __name__ == "__main__":
    unittest.main()
