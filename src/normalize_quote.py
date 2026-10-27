
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any


def _normalize_numeric_field(
    value: Any,
    field_name: str,
) -> int | float | None:
    if value is None:
        return None

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"Campo numérico inválido: {field_name}.")

    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError(f"Campo numérico inválido: {field_name}.") from None

    if not number.is_finite():
        raise ValueError(f"Campo numérico inválido: {field_name}.")

    return value


def normalize_quote(quote: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(quote, dict):
        raise ValueError("Estrutura de cotação inválida.")

    if quote.get("service") != "fx":
        raise ValueError("Serviço inválido.")

    data = quote.get("data")

    if not isinstance(data, list) or len(data) < 8:
        raise ValueError("Estrutura de dados inválida.")

    if data[0] != "Q":
        raise ValueError("A mensagem não é uma cotação válida.")

    symbol_raw = data[1]
    if not isinstance(symbol_raw, str):
        raise ValueError(f"Par cambial inválido: {symbol_raw!r}")

    symbol = symbol_raw.upper()

    if not symbol.isalpha() or len(symbol) != 6:
        raise ValueError(f"Par cambial inválido: {symbol}")

    event_time = datetime.fromisoformat(data[2])

    return {
        "source": "tiingo",
        "symbol": symbol,
        "event_time": event_time.isoformat(),
        "bid_size": _normalize_numeric_field(data[3], "bid_size"),
        "bid_price": _normalize_numeric_field(data[4], "bid_price"),
        "mid_price": _normalize_numeric_field(data[5], "mid_price"),
        "ask_size": _normalize_numeric_field(data[6], "ask_size"),
        "ask_price": _normalize_numeric_field(data[7], "ask_price"),
        "raw_data": data,
    }
