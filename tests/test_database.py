import unittest
from decimal import Decimal
from datetime import datetime, timezone
from unittest.mock import MagicMock

from psycopg.types.json import Jsonb

from src.database import save_quote


class TestDatabase(unittest.TestCase):
    def test_inserts_quote_and_reports_new_record(self):
        connection = MagicMock()
        connection.execute.return_value.rowcount = 1
        quote = {
            "source": "tiingo",
            "symbol": "EURUSD",
            "event_time": "2026-10-22T17:21:09.563000+00:00",
            "received_at": "2026-10-22T17:21:10.000000+00:00",
            "bid_size": 1000000,
            "bid_price": 1.12297,
            "mid_price": 1.122975,
            "ask_size": 1000000,
            "ask_price": 1.12298,
            "raw_data": ["Q", "eurusd", "2026-10-22T17:21:09.563000+00:00"],
        }

        inserted = save_quote(connection, quote)

        self.assertTrue(inserted)
        connection.transaction.assert_called_once()
        params = connection.execute.call_args.args[1]
        self.assertEqual(params[0:2], ("tiingo", "EURUSD"))
        self.assertEqual(
            params[2],
            datetime(2026, 10, 22, 17, 21, 9, 563000, tzinfo=timezone.utc),
        )
        self.assertEqual(
            params[3],
            datetime(2026, 10, 22, 17, 21, 10, tzinfo=timezone.utc),
        )
        self.assertEqual(
            params[4:9],
            tuple(
                map(
                    Decimal,
                    ("1000000", "1.12297", "1.122975", "1000000", "1.12298"),
                )
            ),
        )
        self.assertIsInstance(params[9], Jsonb)

    def test_reports_existing_quote_as_not_inserted(self):
        connection = MagicMock()
        connection.execute.return_value.rowcount = 0
        quote = {
            "source": "tiingo",
            "symbol": "EURUSD",
            "event_time": "2026-10-22T17:21:09.563000+00:00",
            "received_at": "2026-10-22T17:21:10.000000+00:00",
            "bid_size": 1000000,
            "bid_price": 1.12297,
            "mid_price": 1.122975,
            "ask_size": 1000000,
            "ask_price": 1.12298,
            "raw_data": ["Q", "eurusd", "2026-10-22T17:21:09.563000+00:00"],
        }

        self.assertFalse(save_quote(connection, quote))


if __name__ == "__main__":
    unittest.main()
