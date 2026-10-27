"""Load validated daily FX staging CSV records into PostgreSQL."""

import argparse
import csv
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from src.database import connect_database
from src.staging.tiingo_fx_history import validate_staging_file

UPSERT_DAILY_BAR_SQL = """
    INSERT INTO fx_daily_bars (
        source, symbol, event_time, open, high, low, close
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (source, symbol, event_time) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        updated_at = CURRENT_TIMESTAMP
"""


def load_staging_file(staging_file_path: Path) -> int:
    """Atomically upsert all valid staging records and return row count."""
    valid_rows = validate_staging_file(staging_file_path)
    if valid_rows == 0:
        return 0

    with staging_file_path.open(encoding="utf-8", newline="") as staging_file:
        rows = list(csv.DictReader(staging_file))

    with connect_database() as connection:
        with connection.transaction():
            for row in rows:
                connection.execute(
                    UPSERT_DAILY_BAR_SQL,
                    (
                        row["source"],
                        row["symbol"],
                        datetime.fromisoformat(row["event_time"]),
                        Decimal(row["open"]),
                        Decimal(row["high"]),
                        Decimal(row["low"]),
                        Decimal(row["close"]),
                    ),
                )

    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Carrega um CSV de staging para fx_daily_bars.",
    )
    parser.add_argument("staging_file", type=Path)
    args = parser.parse_args()

    loaded_rows = load_staging_file(args.staging_file)
    print(
        f"Carga concluída: {loaded_rows} registo(s) processado(s) "
        "em fx_daily_bars."
    )


if __name__ == "__main__":
    main()
