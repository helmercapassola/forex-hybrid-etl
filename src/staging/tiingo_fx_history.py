"""Prepare Tiingo historical Forex JSON responses as canonical CSV files."""

import argparse
import csv
import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
STAGING_DATA_DIR = PROJECT_ROOT / "data" / "staging"
STAGING_FIELDS = (
    "source",
    "symbol",
    "event_time",
    "open",
    "high",
    "low",
    "close",
)
PRICE_FIELDS = ("open", "high", "low", "close")


def _normalize_record(record: Any, index: int) -> dict[str, str]:
    if not isinstance(record, dict):
        raise ValueError(f"Registo {index}: esperado objecto JSON.")

    required_fields = {"date", "ticker", *PRICE_FIELDS}
    missing_fields = required_fields - record.keys()
    if missing_fields:
        missing = ", ".join(sorted(missing_fields))
        raise ValueError(f"Registo {index}: campos obrigatórios ausentes: {missing}.")

    symbol = record["ticker"]
    if (
        not isinstance(symbol, str)
        or len(symbol) != 6
        or not symbol.isascii()
        or not symbol.isalpha()
    ):
        raise ValueError(f"Registo {index}: ticker Forex inválido.")

    timestamp = record["date"]
    if not isinstance(timestamp, str):
        raise ValueError(f"Registo {index}: data inválida.")
    try:
        event_time = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Registo {index}: data não está em formato ISO.") from exc
    if event_time.tzinfo is None:
        raise ValueError(f"Registo {index}: data sem fuso horário.")

    normalized = {
        "source": "tiingo",
        "symbol": symbol.upper(),
        "event_time": event_time.astimezone(timezone.utc).isoformat(),
    }

    for field in PRICE_FIELDS:
        value = record[field]
        if isinstance(value, bool) or not isinstance(value, (int, float, str)):
            raise ValueError(f"Registo {index}: valor inválido no campo {field}.")
        try:
            price = Decimal(str(value))
        except InvalidOperation as exc:
            raise ValueError(
                f"Registo {index}: valor inválido no campo {field}."
            ) from exc
        if not price.is_finite():
            raise ValueError(f"Registo {index}: valor inválido no campo {field}.")
        normalized[field] = format(price, "f")

    return normalized


def prepare_staging_file(
    raw_file_path: Path,
    output_dir: Path = STAGING_DATA_DIR,
) -> Path:
    """Normalize one raw API response into a timestamped staging CSV."""
    with raw_file_path.open(encoding="utf-8") as raw_file:
        records = json.load(raw_file)

    if not isinstance(records, list):
        raise ValueError("A resposta bruta deve ser uma lista JSON.")

    staged_records = [
        _normalize_record(record, index)
        for index, record in enumerate(records, start=1)
    ]

    output_dir.mkdir(parents=True, exist_ok=True)
    extracted_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = output_dir / f"{raw_file_path.stem}_staging_{extracted_at}.csv"
    with output_path.open("x", encoding="utf-8", newline="") as staging_file:
        writer = csv.DictWriter(staging_file, fieldnames=STAGING_FIELDS)
        writer.writeheader()
        writer.writerows(staged_records)

    return output_path


def validate_staging_file(staging_file_path: Path) -> int:
    """Validate a staging CSV and return its number of unique valid rows."""
    seen_keys: set[tuple[str, str]] = set()
    row_count = 0

    with staging_file_path.open(encoding="utf-8", newline="") as staging_file:
        reader = csv.DictReader(staging_file)
        if tuple(reader.fieldnames or ()) != STAGING_FIELDS:
            raise ValueError("O CSV de staging tem colunas inválidas.")

        for row_number, row in enumerate(reader, start=2):
            if None in row or any(not value for value in row.values()):
                raise ValueError(
                    f"Linha {row_number}: existem campos vazios ou colunas extra."
                )
            if row["source"] != "tiingo":
                raise ValueError(f"Linha {row_number}: source inválido.")

            symbol = row["symbol"]
            if (
                len(symbol) != 6
                or not symbol.isascii()
                or not symbol.isalpha()
                or symbol != symbol.upper()
            ):
                raise ValueError(f"Linha {row_number}: symbol inválido.")

            try:
                event_time = datetime.fromisoformat(
                    row["event_time"].replace("Z", "+00:00")
                )
            except ValueError as exc:
                raise ValueError(
                    f"Linha {row_number}: event_time não está em formato ISO."
                ) from exc
            if event_time.tzinfo is None:
                raise ValueError(f"Linha {row_number}: event_time sem fuso horário.")

            key = (symbol, event_time.astimezone(timezone.utc).isoformat())
            if key in seen_keys:
                raise ValueError(
                    f"Linha {row_number}: registo duplicado para "
                    f"{symbol} em {key[1]}."
                )
            seen_keys.add(key)

            prices: dict[str, Decimal] = {}
            for field in PRICE_FIELDS:
                try:
                    price = Decimal(row[field])
                except InvalidOperation as exc:
                    raise ValueError(
                        f"Linha {row_number}: {field} não é um número válido."
                    ) from exc
                if not price.is_finite() or price <= 0:
                    raise ValueError(
                        f"Linha {row_number}: {field} tem de ser um número positivo."
                    )
                prices[field] = price

            if (
                prices["high"] < max(prices["open"], prices["close"])
                or prices["low"] > min(prices["open"], prices["close"])
                or prices["high"] < prices["low"]
            ):
                raise ValueError(
                    f"Linha {row_number}: valores OHLC são incoerentes."
                )

            row_count += 1

    return row_count


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Prepara um JSON histórico Tiingo para a zona de staging.",
    )
    parser.add_argument("raw_file", type=Path)
    args = parser.parse_args()

    output_path = prepare_staging_file(args.raw_file)
    print(f"Staging concluído. Ficheiro criado em {output_path}.")


if __name__ == "__main__":
    main()
