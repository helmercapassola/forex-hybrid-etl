import json
import os
import time
import unittest
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from kafka import KafkaConsumer, KafkaProducer
from kafka.serializer import JsonSerializer

from src.consumers.forex_consumer import validate_quote
from src.database import connect_database, save_quote
from src.producers import tiingo_ws_producer

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REQUIRED_POSTGRES_SETTINGS = (
    "TEST_POSTGRES_HOST",
    "TEST_POSTGRES_PORT",
    "TEST_POSTGRES_DB",
    "TEST_POSTGRES_USER",
    "TEST_POSTGRES_PASSWORD",
)
INTEGRATION_ENABLED = (
    os.getenv("RUN_POSTGRES_INTEGRATION") == "1"
    and all(os.getenv(name) for name in REQUIRED_POSTGRES_SETTINGS)
    and bool(os.getenv("KAFKA_TEST_BOOTSTRAP_SERVERS"))
)


@unittest.skipUnless(
    INTEGRATION_ENABLED,
    "Configure the isolated PostgreSQL and KAFKA_TEST_BOOTSTRAP_SERVERS to run the realtime end-to-end test.",
)
class TestRealtimePipelineKafkaIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["POSTGRES_HOST"] = os.environ["TEST_POSTGRES_HOST"]
        os.environ["POSTGRES_PORT"] = os.environ["TEST_POSTGRES_PORT"]
        os.environ["POSTGRES_DB"] = os.environ["TEST_POSTGRES_DB"]
        os.environ["POSTGRES_USER"] = os.environ["TEST_POSTGRES_USER"]
        os.environ["POSTGRES_PASSWORD"] = os.environ["TEST_POSTGRES_PASSWORD"]

        with connect_database() as connection:
            database_name = connection.execute(
                "SELECT current_database()"
            ).fetchone()[0]
            if database_name != "forex_integration_test":
                raise RuntimeError(
                    "Integration tests require the dedicated "
                    "'forex_integration_test' database; refusing to run."
                )

            schema_path = PROJECT_ROOT / "sql" / "schema.sql"
            connection.execute(schema_path.read_text(encoding="utf-8"))
            connection.execute("TRUNCATE TABLE fx_quotes")

    def test_quote_flows_from_producer_through_kafka_to_postgres(self):
        topic = f"forex-e2e-{uuid.uuid4().hex}"
        event_time = datetime.now(timezone.utc).isoformat()
        websocket_message = json.dumps({
            "service": "fx",
            "messageType": "A",
            "data": [
                "Q",
                "eurusd",
                event_time,
                1000000.0,
                1.12297,
                1.122975,
                1000000.0,
                1.12298,
            ],
        })
        producer = KafkaProducer(
            bootstrap_servers=os.environ[
                "KAFKA_TEST_BOOTSTRAP_SERVERS"
            ].split(","),
            value_serializer=JsonSerializer(),
            acks="all",
            retries=5,
        )
        consumer = None

        try:
            with patch.object(tiingo_ws_producer, "KAFKA_TOPIC", topic):
                tiingo_ws_producer.on_message(
                    None,
                    websocket_message,
                    producer,
                )
            producer.flush(timeout=30)

            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=os.environ[
                    "KAFKA_TEST_BOOTSTRAP_SERVERS"
                ].split(","),
                group_id=f"forex-e2e-{uuid.uuid4().hex}",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                consumer_timeout_ms=1000,
            )

            deadline = time.monotonic() + 30
            quote = None
            while time.monotonic() < deadline and quote is None:
                for message in consumer:
                    quote = validate_quote(message.value)
                    break

            self.assertIsNotNone(quote, "Kafka did not deliver the published quote.")
            self.assertEqual(quote["symbol"], "EURUSD")
            self.assertEqual(quote["event_time"], event_time)

            with connect_database() as connection:
                inserted = save_quote(connection, quote)
                duplicate_inserted = save_quote(connection, quote)
                persisted = connection.execute(
                    """
                    SELECT COUNT(*), MIN(symbol), MIN(bid_price), MIN(ask_price)
                    FROM fx_quotes
                    WHERE source = %s AND symbol = %s AND event_time = %s
                    """,
                    ("tiingo", quote["symbol"], quote["event_time"]),
                ).fetchone()

            self.assertTrue(inserted)
            self.assertFalse(duplicate_inserted)
            self.assertEqual(
                persisted,
                (1, "EURUSD", Decimal("1.12297"), Decimal("1.12298")),
            )
        finally:
            if consumer is not None:
                consumer.close()
            producer.close(timeout=30)


if __name__ == "__main__":
    unittest.main()
