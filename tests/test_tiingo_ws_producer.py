
import json
import unittest
from unittest.mock import Mock, patch

from kafka.serializer import JsonSerializer

from src.producers.tiingo_ws_producer import (
    main,
    normalize_message,
    on_error,
    on_message,
)


class TestTiingoWsProducer(unittest.TestCase):
    def test_normalizes_valid_json_message(self):
        message = json.dumps({
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
        })

        result = normalize_message(message)

        self.assertEqual(result["source"], "tiingo")
        self.assertEqual(result["symbol"], "EURUSD")
        self.assertEqual(
            result["event_time"],
            "2026-10-22T17:21:09.563000+00:00",
        )
        self.assertIn("received_at", result)

    def test_handles_invalid_json(self):
        result = normalize_message("mensagem-invalida")

        self.assertTrue(result["parse_error"])
        self.assertEqual(result["raw_message"], "mensagem-invalida")

    def test_handles_invalid_quote(self):
        message = json.dumps({
            "service": "fx",
            "messageType": "A",
            "data": [
                "T",
                "eurusd",
                "2026-10-22T17:21:09.563000+00:00",
            ],
        })

        result = normalize_message(message)

        self.assertIn("normalization_error", result)
        self.assertEqual(result["symbol"], "EURUSD")

    def test_publishes_valid_quote_to_kafka(self):
        message = json.dumps({
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
        })
        producer = Mock()
        future = producer.send.return_value

        on_message(None, message, producer)

        producer.send.assert_called_once()
        self.assertEqual(producer.send.call_args.args[0], "forex-quotes")
        self.assertEqual(
            producer.send.call_args.kwargs["key"],
            b"EURUSD:2026-10-22T17:21:09.563000+00:00",
        )
        quote = producer.send.call_args.kwargs["value"]
        self.assertEqual(quote["symbol"], "EURUSD")
        future.add_callback.assert_called_once()
        future.add_errback.assert_called_once()

    def test_reconnect_delay_increases_and_is_capped(self):
        with (
            patch(
                "src.producers.tiingo_ws_producer.KafkaProducer"
            ) as kafka_producer,
            patch(
                "src.producers.tiingo_ws_producer.websocket.WebSocketApp"
            ) as websocket_app,
            patch("src.producers.tiingo_ws_producer.time.sleep") as sleep,
        ):
            websocket_app.return_value.run_forever.side_effect = (
                [None] * 8 + [KeyboardInterrupt]
            )

            main()

        kafka_producer.return_value.flush.assert_called_once()
        kafka_producer.return_value.close.assert_called_once()
        self.assertTrue(kafka_producer.call_args.kwargs["enable_idempotence"])
        self.assertEqual(kafka_producer.call_args.kwargs["acks"], "all")
        self.assertIsInstance(
            kafka_producer.call_args.kwargs["value_serializer"],
            JsonSerializer,
        )
        self.assertEqual(
            [call.args[0] for call in sleep.call_args_list],
            [1, 2, 4, 8, 16, 32, 60, 60],
        )

    def test_websocket_error_log_includes_type_and_redacts_token(self):
        error = RuntimeError("connection failed: test-token")

        with (
            patch(
                "src.producers.tiingo_ws_producer.TIINGO_API_TOKEN",
                "test-token",
            ),
            self.assertLogs(
                "src.producers.tiingo_ws_producer",
                level="ERROR",
            ) as captured,
        ):
            on_error(None, error)

        self.assertIn("RuntimeError", captured.output[0])
        self.assertIn("[REDACTED]", captured.output[0])
        self.assertNotIn("test-token", captured.output[0])

    def test_keyboard_interrupt_stops_without_reconnecting(self):
        clients = []

        def create_websocket_app(*args, **kwargs):
            client = Mock()
            client.run_forever.side_effect = lambda **options: kwargs[
                "on_error"
            ](client, KeyboardInterrupt())
            clients.append(client)
            return client

        with (
            patch(
                "src.producers.tiingo_ws_producer.KafkaProducer"
            ) as kafka_producer,
            patch(
                "src.producers.tiingo_ws_producer.websocket.WebSocketApp",
                side_effect=create_websocket_app,
            ),
            patch("src.producers.tiingo_ws_producer.time.sleep") as sleep,
        ):
            main()

        self.assertEqual(len(clients), 1)
        sleep.assert_not_called()
        kafka_producer.return_value.flush.assert_called_once()
        kafka_producer.return_value.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
