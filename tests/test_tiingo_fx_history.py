import io
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from src.extractors.tiingo_fx_history import (
    fetch_historical_quotes,
    save_raw_response,
)


class TestTiingoFxHistory(unittest.TestCase):
    @patch("src.extractors.tiingo_fx_history.TIINGO_API_TOKEN", "test-token")
    @patch("src.extractors.tiingo_fx_history.urllib.request.urlopen")
    def test_fetches_daily_records_with_token_and_date_range(self, urlopen):
        payload = [
            {
                "date": "2026-10-27T00:00:00.000Z",
                "ticker": "eurusd",
                "open": 1.11736,
                "high": 1.117695,
                "low": 1.11627,
                "close": 1.11713,
            },
        ]
        response = io.BytesIO(json.dumps(payload).encode("utf-8"))
        urlopen.return_value.__enter__.return_value = response

        result = fetch_historical_quotes(
            "EURUSD",
            date(2026, 10, 27),
            date(2026, 10, 27),
        )

        self.assertEqual(result, payload)
        request = urlopen.call_args.args[0]
        self.assertEqual(
            request.full_url,
            "https://api.tiingo.com/tiingo/fx/eurusd/prices?"
            "startDate=2026-10-27&endDate=2026-10-27&resampleFreq=1day",
        )
        self.assertEqual(
            request.get_header("Authorization"),
            "Token test-token",
        )
        self.assertEqual(request.get_header("Accept"), "application/json")
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 30)

    def test_rejects_invalid_symbol(self):
        with self.assertRaisesRegex(ValueError, "seis letras ASCII"):
            fetch_historical_quotes(
                "EUR/USD",
                date(2026, 10, 27),
                date(2026, 10, 27),
            )

    def test_rejects_start_date_after_end_date(self):
        with self.assertRaisesRegex(ValueError, "posterior"):
            fetch_historical_quotes(
                "EURUSD",
                date(2026, 10, 28),
                date(2026, 10, 27),
            )

    def test_saves_raw_response_to_timestamped_json_file(self):
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
            output_path = save_raw_response(
                records,
                "EURUSD",
                date(2026, 10, 27),
                date(2026, 10, 27),
                Path(temporary_directory),
            )

            self.assertRegex(
                output_path.name,
                r"^tiingo_fx_eurusd_2026-10-27_2026-10-27_"
                r"\d{8}T\d{6}Z\.json$",
            )
            self.assertEqual(
                json.loads(output_path.read_text(encoding="utf-8")),
                records,
            )

    @patch("src.extractors.tiingo_fx_history.urllib.request.urlopen")
    def test_rejects_non_list_api_response(self, urlopen):
        response = io.BytesIO(b'{"detail":"invalid response"}')
        urlopen.return_value.__enter__.return_value = response

        with self.assertRaisesRegex(ValueError, "não é uma lista"):
            fetch_historical_quotes(
                "EURUSD",
                date(2026, 10, 27),
                date(2026, 10, 27),
            )


if __name__ == "__main__":
    unittest.main()
