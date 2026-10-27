
import unittest

from src.normalize_quote import normalize_quote


class TestNormalizeQuote(unittest.TestCase):
    def setUp(self):
        self.valid_quote = {
            "service": "fx",
            "messageType": "A",
            "data": [
                "Q",
                "eurusd",
                "2026-10-22T17:21:09.563000+00:00",
                1000000.0,
                1.12297,
                1.122975,
                1000000.0,
                1.12298,
            ],
        }

    def test_normalizes_valid_quote(self):
        result = normalize_quote(self.valid_quote)

        self.assertEqual(result["source"], "tiingo")
        self.assertEqual(result["symbol"], "EURUSD")
        self.assertEqual(
            result["event_time"],
            "2026-10-22T17:21:09.563000+00:00",
        )
        self.assertEqual(result["bid_size"], 1000000.0)
        self.assertEqual(result["bid_price"], 1.12297)
        self.assertEqual(result["mid_price"], 1.122975)
        self.assertEqual(result["ask_size"], 1000000.0)
        self.assertEqual(result["ask_price"], 1.12298)
        self.assertEqual(result["raw_data"], self.valid_quote["data"])

    def test_rejects_quote_without_all_documented_fields(self):
        quote = self.valid_quote.copy()
        quote["data"] = quote["data"][:7]

        with self.assertRaises(ValueError):
            normalize_quote(quote)

    def test_rejects_non_numeric_price_field(self):
        quote = self.valid_quote.copy()
        quote["data"] = quote["data"].copy()
        quote["data"][4] = "not-a-price"

        with self.assertRaises(ValueError):
            normalize_quote(quote)

    def test_rejects_invalid_message_type(self):
        quote = self.valid_quote.copy()
        quote["data"] = ["T", "eurusd", "2026-10-22T17:21:09.563000+00:00"]

        with self.assertRaises(ValueError):
            normalize_quote(quote)

    def test_rejects_invalid_service(self):
        quote = self.valid_quote.copy()
        quote["service"] = "crypto"

        with self.assertRaises(ValueError):
            normalize_quote(quote)


if __name__ == "__main__":
    unittest.main()
