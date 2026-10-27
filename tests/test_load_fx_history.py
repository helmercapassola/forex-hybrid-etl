import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.loaders.load_fx_history import load_staging_file


VALID_CSV = (
    "source,symbol,event_time,open,high,low,close\n"
    "tiingo,EURUSD,2026-10-27T00:00:00+00:00,"
    "1.1,1.2,1.0,1.15\n"
)


class TestLoadFxHistory(unittest.TestCase):
    @patch("src.loaders.load_fx_history.connect_database")
    def test_upserts_valid_rows_in_one_transaction(self, connect_database):
        with tempfile.TemporaryDirectory() as temporary_directory:
            staging_file = Path(temporary_directory) / "staging.csv"
            staging_file.write_text(VALID_CSV, encoding="utf-8")
            connection = connect_database.return_value.__enter__.return_value

            loaded_rows = load_staging_file(staging_file)

        self.assertEqual(loaded_rows, 1)
        connection.transaction.assert_called_once()
        connection.execute.assert_called_once()
        sql, params = connection.execute.call_args.args
        self.assertIn("ON CONFLICT (source, symbol, event_time) DO UPDATE", sql)
        self.assertEqual(
            params,
            (
                "tiingo",
                "EURUSD",
                unittest.mock.ANY,
                unittest.mock.ANY,
                unittest.mock.ANY,
                unittest.mock.ANY,
                unittest.mock.ANY,
            ),
        )

    @patch("src.loaders.load_fx_history.connect_database")
    def test_rejects_bad_file_before_connecting(self, connect_database):
        invalid_csv = (
            "source,symbol,event_time,open,high,low,close\n"
            "tiingo,EURUSD,2026-10-27T00:00:00+00:00,"
            "1.1,1.05,1.0,1.15\n"
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            staging_file = Path(temporary_directory) / "staging.csv"
            staging_file.write_text(invalid_csv, encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "OHLC são incoerentes"):
                load_staging_file(staging_file)

        connect_database.assert_not_called()

    @patch("src.loaders.load_fx_history.connect_database")
    def test_empty_staging_file_does_not_connect(self, connect_database):
        with tempfile.TemporaryDirectory() as temporary_directory:
            staging_file = Path(temporary_directory) / "staging.csv"
            staging_file.write_text(
                "source,symbol,event_time,open,high,low,close\n",
                encoding="utf-8",
            )

            self.assertEqual(load_staging_file(staging_file), 0)

        connect_database.assert_not_called()


if __name__ == "__main__":
    unittest.main()
