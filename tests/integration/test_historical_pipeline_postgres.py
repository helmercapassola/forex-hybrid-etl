import http.server
import json
import os
import socket
import threading
import tempfile
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

import psycopg

import src.extractors.tiingo_fx_history as tiingo_history
from src.database import connect_database
from src.loaders.load_fx_history import load_staging_file
from src.staging.tiingo_fx_history import prepare_staging_file

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REQUIRED_TEST_SETTINGS = (
    "TEST_POSTGRES_HOST",
    "TEST_POSTGRES_PORT",
    "TEST_POSTGRES_DB",
    "TEST_POSTGRES_USER",
    "TEST_POSTGRES_PASSWORD",
)
INTEGRATION_ENABLED = (
    os.getenv("RUN_POSTGRES_INTEGRATION") == "1"
    and all(os.getenv(name) for name in REQUIRED_TEST_SETTINGS)
)


@unittest.skipUnless(
    INTEGRATION_ENABLED,
    "Set RUN_POSTGRES_INTEGRATION=1 and TEST_POSTGRES_* to run isolated PostgreSQL integration tests.",
)
class TestHistoricalPipelinePostgresIntegration(unittest.TestCase):
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

            migration_path = (
                PROJECT_ROOT
                / "sql"
                / "migrations"
                / "002_create_fx_daily_bars.sql"
            )
            connection.execute(
                migration_path.read_text(encoding="utf-8")
            )

    def setUp(self):
        with connect_database() as connection:
            connection.execute("TRUNCATE TABLE fx_daily_bars")

    def test_extract_stage_and_load_are_persisted_and_idempotent(self):
        initial_record = {
            "date": "2026-10-27T00:00:00.000Z",
            "ticker": "eurusd",
            "open": 1.1,
            "high": 1.2,
            "low": 1.0,
            "close": 1.15,
        }

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            raw_file = root / "initial.json"
            raw_file.write_text(
                json.dumps([initial_record]),
                encoding="utf-8",
            )
            staging_file = prepare_staging_file(raw_file, root / "staging")

            self.assertEqual(load_staging_file(staging_file), 1)
            self.assertEqual(load_staging_file(staging_file), 1)

            updated_record = {
                **initial_record,
                "close": 1.16,
            }
            updated_raw_file = root / "updated.json"
            updated_raw_file.write_text(
                json.dumps([updated_record]),
                encoding="utf-8",
            )
            updated_staging_file = prepare_staging_file(
                updated_raw_file,
                root / "staging",
            )
            self.assertEqual(load_staging_file(updated_staging_file), 1)

        with connect_database() as connection:
            rows = connection.execute(
                """
                SELECT symbol, event_time, open, high, low, close
                FROM fx_daily_bars
                WHERE source = %s AND symbol = %s
                """,
                ("tiingo", "EURUSD"),
            ).fetchall()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "EURUSD")
        self.assertEqual(rows[0][1].date(), date(2026, 10, 27))
        self.assertEqual(rows[0][2:], (
            Decimal("1.1"),
            Decimal("1.2"),
            Decimal("1.0"),
            Decimal("1.16"),
        ))

    def test_invalid_staging_data_never_reaches_postgres(self):
        invalid_csv = (
            "source,symbol,event_time,open,high,low,close\n"
            "tiingo,EURUSD,2026-10-27T00:00:00+00:00,"
            "1.1,1.05,1.0,1.15\n"
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            staging_file = Path(temporary_directory) / "invalid.csv"
            staging_file.write_text(invalid_csv, encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "OHLC são incoerentes"):
                load_staging_file(staging_file)

        with connect_database() as connection:
            row_count = connection.execute(
                "SELECT COUNT(*) FROM fx_daily_bars"
            ).fetchone()[0]

        self.assertEqual(row_count, 0)

    def test_postgres_unavailable_is_reported_without_success_fallback(self):
        valid_csv = (
            "source,symbol,event_time,open,high,low,close\n"
            "tiingo,EURUSD,2026-10-27T00:00:00+00:00,"
            "1.1,1.2,1.0,1.15\n"
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            staging_file = Path(temporary_directory) / "valid.csv"
            staging_file.write_text(valid_csv, encoding="utf-8")

            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                unavailable_port = listener.getsockname()[1]

            previous_port = os.environ["POSTGRES_PORT"]
            os.environ["POSTGRES_PORT"] = str(unavailable_port)
            try:
                with self.assertRaises(psycopg.OperationalError):
                    load_staging_file(staging_file)
            finally:
                os.environ["POSTGRES_PORT"] = previous_port


class TestTiingoUnavailableIntegration(unittest.TestCase):
    @unittest.skipUnless(
        INTEGRATION_ENABLED,
        "Set RUN_POSTGRES_INTEGRATION=1 and TEST_POSTGRES_* to run isolated dependency tests.",
    )
    def test_http_service_error_is_propagated_by_extractor(self):
        class ErrorHandler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(503, "Service Unavailable")
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, format, *args):
                return

        server = http.server.ThreadingHTTPServer(
            ("127.0.0.1", 0),
            ErrorHandler,
        )
        server_thread = threading.Thread(
            target=server.serve_forever,
            daemon=True,
        )
        server_thread.start()
        try:
            with (
                patch.object(tiingo_history, "BASE_URL",
                             f"http://127.0.0.1:{server.server_port}/tiingo/fx"),
                patch.object(tiingo_history, "TIINGO_API_TOKEN", "integration-test-token"),
            ):
                with self.assertRaises(HTTPError) as error:
                    tiingo_history.fetch_historical_quotes(
                        "EURUSD",
                        date(2026, 10, 27),
                        date(2026, 10, 27),
                    )

            self.assertEqual(error.exception.code, 503)
        finally:
            server.shutdown()
            server.server_close()
            server_thread.join(timeout=5)
            self.assertFalse(server_thread.is_alive())


if __name__ == "__main__":
    unittest.main()
