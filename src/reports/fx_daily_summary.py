"""Generate CSV summaries for daily Forex bars stored in PostgreSQL."""

import csv
from datetime import date, datetime, timezone
from pathlib import Path

from src.database import connect_database

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPORT_DATA_DIR = PROJECT_ROOT / "data" / "reports"
REPORT_FIELDS = (
    "source",
    "symbol",
    "requested_start_date",
    "requested_end_date",
    "first_date_with_data",
    "last_date_with_data",
    "number_of_bars",
    "bars_with_previous_close",
    "average_change_pct",
    "minimum_change_pct",
    "maximum_change_pct",
)

DAILY_SUMMARY_SQL = """
    WITH candles_with_previous_close AS (
        SELECT
            source,
            symbol,
            event_time,
            close,
            LAG(close) OVER (
                PARTITION BY source, symbol
                ORDER BY event_time
            ) AS previous_close
        FROM fx_daily_bars
        WHERE source = %s
          AND symbol = %s
    )
    SELECT
        %s::text AS source,
        %s::text AS symbol,
        MIN((event_time AT TIME ZONE 'UTC')::date) AS first_date_with_data,
        MAX((event_time AT TIME ZONE 'UTC')::date) AS last_date_with_data,
        COUNT(*) AS number_of_bars,
        COUNT(previous_close) AS bars_with_previous_close,
        ROUND(
            AVG(
                ((close - previous_close) / NULLIF(previous_close, 0) * 100)::numeric
            ),
            4
        ) AS average_change_pct,
        ROUND(
            MIN(
                ((close - previous_close) / NULLIF(previous_close, 0) * 100)::numeric
            ),
            4
        ) AS minimum_change_pct,
        ROUND(
            MAX(
                ((close - previous_close) / NULLIF(previous_close, 0) * 100)::numeric
            ),
            4
        ) AS maximum_change_pct
    FROM candles_with_previous_close
    WHERE (event_time AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
"""


def create_daily_summary_report(
    symbol: str,
    start_date: date,
    end_date: date,
    output_dir: Path = REPORT_DATA_DIR,
) -> Path:
    """Write a one-row summary CSV for one symbol and inclusive date range."""
    normalized_symbol = symbol.strip().upper()
    if (
        len(normalized_symbol) != 6
        or not normalized_symbol.isascii()
        or not normalized_symbol.isalpha()
    ):
        raise ValueError("O símbolo Forex deve conter seis letras ASCII.")
    if start_date > end_date:
        raise ValueError("A data inicial não pode ser posterior à data final.")

    source = "tiingo"
    with connect_database() as connection:
        result = connection.execute(
            DAILY_SUMMARY_SQL,
            (
                source,
                normalized_symbol,
                source,
                normalized_symbol,
                start_date,
                end_date,
            ),
        ).fetchone()

    if result is None:
        raise RuntimeError("A consulta do resumo não devolveu uma linha.")

    summary = dict(zip(REPORT_FIELDS[:2] + REPORT_FIELDS[4:], result))
    summary["requested_start_date"] = start_date.isoformat()
    summary["requested_end_date"] = end_date.isoformat()

    output_dir.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    output_path = output_dir / (
        f"fx_daily_summary_{normalized_symbol.lower()}_"
        f"{start_date.isoformat()}_{end_date.isoformat()}_{generated_at}.csv"
    )
    with output_path.open("x", encoding="utf-8", newline="") as report_file:
        writer = csv.DictWriter(report_file, fieldnames=REPORT_FIELDS)
        writer.writeheader()
        writer.writerow(summary)

    return output_path
