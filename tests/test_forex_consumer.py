import json
import unittest
from unittest.mock import Mock, patch

from kafka import TopicPartition
from kafka.structs import OffsetAndMetadata

from src.consumers.forex_consumer import main, validate_quote


class TestForexConsumer(unittest.TestCase):
    @patch("src.consumers.forex_consumer.save_quote")
    @patch("src.consumers.forex_consumer.connect_database")
    @patch("src.consumers.forex_consumer.KafkaConsumer")
    def test_persists_before_committing_and_closes_resources(
        self,
        kafka_consumer,
        connect_database,
        save_quote,
    ):
        quote = {
            "source": "tiingo",
            "symbol": "EURUSD",
            "event_time": "2026-10-22T17:21:09.563000+00:00",
            "received_at": "2026-10-22T17:21:10.000000+00:00",
            "raw_data": [
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
        database = connect_database.return_value
        consumer = kafka_consumer.return_value
        message = Mock(
            value=json.dumps(quote).encode("utf-8"),
            topic="forex-quotes",
            partition=0,
            offset=3,
        )
        consumer.__iter__.return_value = iter([message])
        operations = []
        save_quote.side_effect = lambda *_args: operations.append("save")
        consumer.commit.side_effect = lambda *_args: operations.append("commit")

        with self.assertLogs(
            "src.consumers.forex_consumer",
            level="INFO",
        ):
            main()

        save_quote.assert_called_once_with(
            database,
            {
                **quote,
                "bid_size": 1000000.0,
                "bid_price": 1.12297,
                "mid_price": 1.122975,
                "ask_size": 1000000.0,
                "ask_price": 1.12298,
            },
        )
        self.assertEqual(operations, ["save", "commit"])
        consumer.commit.assert_called_once_with({
            TopicPartition("forex-quotes", 0): OffsetAndMetadata(4, ""),
        })
        consumer.close.assert_called_once()
        database.close.assert_called_once()

    @patch("src.consumers.forex_consumer.connect_database")
    @patch("src.consumers.forex_consumer.KafkaConsumer")
    def test_keyboard_interrupt_closes_consumer_cleanly(
        self,
        kafka_consumer,
        connect_database,
    ):
        consumer = kafka_consumer.return_value
        consumer.__iter__.side_effect = KeyboardInterrupt

        with self.assertLogs(
            "src.consumers.forex_consumer",
            level="INFO",
        ) as captured:
            main()

        self.assertIn("interrompido pelo utilizador", captured.output[0])
        consumer.close.assert_called_once()
        connect_database.return_value.close.assert_called_once()

    def test_rejects_malformed_json(self):
        with self.assertRaisesRegex(ValueError, "valid UTF-8 JSON"):
            validate_quote(b"{invalid")

    def test_rejects_invalid_quote_fields(self):
        payload = {
            "source": "tiingo",
            "symbol": "EURUSD",
            "event_time": "not-a-date",
            "raw_data": [
                "Q",
                "eurusd",
                "not-a-date",
                1000000.0,
                1.12297,
                1.122975,
                1000000.0,
                1.12298,
            ],
        }

        with self.assertRaisesRegex(ValueError, "ISO timestamp"):
            validate_quote(json.dumps(payload).encode("utf-8"))

    def test_rejects_raw_timestamp_mismatch(self):
        payload = {
            "source": "tiingo",
            "symbol": "EURUSD",
            "event_time": "2026-10-22T17:21:09.563000+00:00",
            "raw_data": [
                "Q",
                "eurusd",
                "2026-10-22T17:21:10+00:00",
                1000000.0,
                1.12297,
                1.122975,
                1000000.0,
                1.12298,
            ],
        }

        with self.assertRaisesRegex(ValueError, "does not match event_time"):
            validate_quote(json.dumps(payload).encode("utf-8"))

    @patch("src.consumers.forex_consumer.save_quote")
    @patch("src.consumers.forex_consumer.connect_database")
    @patch("src.consumers.forex_consumer.KafkaConsumer")
    def test_logs_and_commits_invalid_record_to_avoid_poison_loop(
        self,
        kafka_consumer,
        connect_database,
        save_quote,
    ):
        consumer = kafka_consumer.return_value
        message = Mock(
            value=b"not-json",
            topic="forex-quotes",
            partition=1,
            offset=8,
        )
        consumer.__iter__.return_value = iter([message])

        with self.assertLogs(
            "src.consumers.forex_consumer",
            level="WARNING",
        ) as captured:
            main()

        self.assertIn("Mensagem inválida ignorada", captured.output[0])
        save_quote.assert_not_called()
        consumer.commit.assert_called_once_with({
            TopicPartition("forex-quotes", 1): OffsetAndMetadata(9, ""),
        })
        connect_database.return_value.close.assert_called_once()

    @patch("src.consumers.forex_consumer.save_quote")
    @patch("src.consumers.forex_consumer.connect_database")
    @patch("src.consumers.forex_consumer.KafkaConsumer")
    def test_database_failure_does_not_commit_offset(
        self,
        kafka_consumer,
        connect_database,
        save_quote,
    ):
        quote = {
            "source": "tiingo",
            "symbol": "EURUSD",
            "event_time": "2026-10-22T17:21:09.563000+00:00",
            "received_at": "2026-10-22T17:21:10.000000+00:00",
            "raw_data": [
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
        consumer = kafka_consumer.return_value
        consumer.__iter__.return_value = iter([
            Mock(
                value=json.dumps(quote).encode("utf-8"),
                topic="forex-quotes",
                partition=0,
                offset=5,
            ),
        ])
        save_quote.side_effect = RuntimeError("database unavailable")

        with self.assertRaisesRegex(RuntimeError, "database unavailable"):
            main()

        consumer.commit.assert_not_called()
        consumer.close.assert_called_once()
        connect_database.return_value.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
