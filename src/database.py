import os
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
from dotenv import load_dotenv
from psycopg.types.json import Jsonb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

INSERT_QUOTE_SQL = """
    INSERT INTO fx_quotes (
        source, symbol, event_time, received_at,
        bid_size, bid_price, mid_price, ask_size, ask_price, raw_data
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (source, symbol, event_time, raw_data) DO NOTHING
"""


def connect_database() -> psycopg.Connection:
    """Connect to PostgreSQL using the project's local environment settings."""
    required_settings = {
        "POSTGRES_DB": os.getenv("POSTGRES_DB"),
        "POSTGRES_USER": os.getenv("POSTGRES_USER"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD"),
    }
    missing_settings = [
        name for name, value in required_settings.items() if not value
    ]
    if missing_settings:
        names = ", ".join(missing_settings)
        raise ValueError(f"Missing required PostgreSQL settings: {names}")

    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=required_settings["POSTGRES_DB"],
        user=required_settings["POSTGRES_USER"],
        password=required_settings["POSTGRES_PASSWORD"],
        connect_timeout=10,
    )


def save_quote(connection: psycopg.Connection, quote: dict[str, Any]) -> bool:
    """Insert one quote idempotently; return False if it was already stored."""
    event_time = datetime.fromisoformat(quote["event_time"])
    received_at = datetime.fromisoformat(quote["received_at"])

    with connection.transaction():
        cursor = connection.execute(
            INSERT_QUOTE_SQL,
            (
                quote["source"],
                quote["symbol"],
                event_time,
                received_at,
                *(
                    Decimal(str(quote[field])) if quote[field] is not None else None
                    for field in (
                        "bid_size",
                        "bid_price",
                        "mid_price",
                        "ask_size",
                        "ask_price",
                    )
                ),
                Jsonb(quote["raw_data"]),
            ),
        )

    return cursor.rowcount == 1
