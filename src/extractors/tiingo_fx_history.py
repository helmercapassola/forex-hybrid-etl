"""Extract daily historical Forex prices from the Tiingo REST API."""

import argparse
import json
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from src.config import FOREX_SYMBOL, TIINGO_API_TOKEN

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
BASE_URL = "https://api.tiingo.com/tiingo/fx"
RESAMPLE_FREQUENCY = "1day"
REQUEST_TIMEOUT_SECONDS = 30


def fetch_historical_quotes(
    symbol: str,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    """Fetch daily OHLC candles for one FX symbol and inclusive date range."""
    normalized_symbol = symbol.strip().lower()
    if (
        len(normalized_symbol) != 6
        or not normalized_symbol.isascii()
        or not normalized_symbol.isalpha()
    ):
        raise ValueError("O símbolo Forex deve conter seis letras ASCII.")

    if start_date > end_date:
        raise ValueError("A data inicial não pode ser posterior à data final.")

    query = urllib.parse.urlencode(
        {
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
            "resampleFreq": RESAMPLE_FREQUENCY,
        }
    )
    request = urllib.request.Request(
        f"{BASE_URL}/{normalized_symbol}/prices?{query}",
        headers={
            "Authorization": f"Token {TIINGO_API_TOKEN}",
            "Accept": "application/json",
        },
    )

    with urllib.request.urlopen(
        request,
        timeout=REQUEST_TIMEOUT_SECONDS,
    ) as response:
        payload = json.load(response)

    if not isinstance(payload, list):
        raise ValueError("A resposta histórica da Tiingo não é uma lista.")

    if any(not isinstance(record, dict) for record in payload):
        raise ValueError("A resposta histórica contém registos inválidos.")

    return payload


def save_raw_response(
    records: list[dict[str, Any]],
    symbol: str,
    start_date: date,
    end_date: date,
    output_dir: Path = RAW_DATA_DIR,
) -> Path:
    """Save an untransformed API response as a timestamped JSON file."""
    output_dir.mkdir(parents=True, exist_ok=True)
    extracted_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = (
        f"tiingo_fx_{symbol.strip().lower()}_"
        f"{start_date.isoformat()}_{end_date.isoformat()}_{extracted_at}.json"
    )
    output_path = output_dir / filename
    with output_path.open("x", encoding="utf-8", newline="\n") as raw_file:
        json.dump(records, raw_file, ensure_ascii=False, indent=2)
        raw_file.write("\n")

    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extrai velas Forex diárias do endpoint histórico Tiingo.",
    )
    parser.add_argument("--start-date", required=True, type=date.fromisoformat)
    parser.add_argument("--end-date", required=True, type=date.fromisoformat)
    parser.add_argument("--symbol", default=FOREX_SYMBOL)
    args = parser.parse_args()

    records = fetch_historical_quotes(
        args.symbol,
        args.start_date,
        args.end_date,
    )
    output_path = save_raw_response(
        records,
        args.symbol,
        args.start_date,
        args.end_date,
    )
    print(
        f"Extração concluída: {len(records)} registo(s) "
        f"para {args.symbol.upper()} entre "
        f"{args.start_date} e {args.end_date}. "
        f"Resposta bruta guardada em {output_path.relative_to(PROJECT_ROOT)}."
    )


if __name__ == "__main__":
    main()
