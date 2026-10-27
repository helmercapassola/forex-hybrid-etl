import csv
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from src.reports.fx_daily_summary import (
    REPORT_FIELDS,
    create_daily_summary_report,
)


class TestCreateDailySummaryReport(unittest.TestCase):
    @patch("src.reports.fx_daily_summary.connect_database")
    def test_writes_summary_csv_for_requested_range(self, connect_database):
        connection = connect_database.return_value.__enter__.return_value
        connection.execute.return_value.fetchone.return_value = (
            "tiingo",
            "EURUSD",
            date(2026, 10, 20),
            date(2026, 10, 26),
            6,
            5,
            "-0.0935",
            "-0.3905",
            "0.1209",
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            report_path = create_daily_summary_report(
                "eurusd",
                date(2026, 10, 20),
                date(2026, 10, 26),
                Path(temporary_directory),
            )

            with report_path.open(encoding="utf-8", newline="") as report_file:
                reader = csv.DictReader(report_file)
                rows = list(reader)

        self.assertEqual(tuple(reader.fieldnames or ()), REPORT_FIELDS)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["symbol"], "EURUSD")
        self.assertEqual(rows[0]["requested_start_date"], "2026-10-20")
        self.assertEqual(rows[0]["requested_end_date"], "2026-10-26")
        self.assertEqual(rows[0]["number_of_bars"], "6")
        self.assertEqual(rows[0]["average_change_pct"], "-0.0935")

        _, parameters = connection.execute.call_args.args
        self.assertEqual(parameters[0:4], ("tiingo", "EURUSD", "tiingo", "EURUSD"))
        self.assertEqual(parameters[4:], (date(2026, 10, 20), date(2026, 10, 26)))

    @patch("src.reports.fx_daily_summary.connect_database")
    def test_rejects_invalid_symbol_before_connecting(self, connect_database):
        with self.assertRaisesRegex(ValueError, "seis letras ASCII"):
            create_daily_summary_report(
                "EUR/USD",
                date(2026, 10, 20),
                date(2026, 10, 26),
                Path("unused"),
            )

        connect_database.assert_not_called()

    @patch("src.reports.fx_daily_summary.connect_database")
    def test_rejects_reversed_date_range_before_connecting(self, connect_database):
        with self.assertRaisesRegex(ValueError, "data inicial"):
            create_daily_summary_report(
                "EURUSD",
                date(2026, 10, 27),
                date(2026, 10, 26),
                Path("unused"),
            )

        connect_database.assert_not_called()


if __name__ == "__main__":
    unittest.main()
