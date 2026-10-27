import json
import logging
from datetime import datetime

from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata

from src.database import connect_database, save_quote
from src.normalize_quote import normalize_quote

logger = logging.getLogger(__name__)

KAFKA_TOPIC = "forex-quotes"
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
CONSUMER_GROUP_ID = "forex-consumer-group"


def validate_quote(value: bytes) -> dict:
    """Decode and validate one Kafka record as a normalized Tiingo quote."""
    if not isinstance(value, bytes):
        raise ValueError("Quote payload must be bytes.")

    try:
        quote = json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Payload is not valid UTF-8 JSON.") from exc

    if not isinstance(quote, dict):
        raise ValueError("Quote payload must be a JSON object.")

    if quote.get("source") != "tiingo":
        raise ValueError("Quote source must be 'tiingo'.")

    symbol = quote.get("symbol")
    if (
        not isinstance(symbol, str)
        or len(symbol) != 6
        or not symbol.isascii()
        or not symbol.isalpha()
    ):
        raise ValueError("Quote symbol must contain six letters.")

    event_time = quote.get("event_time")
    if not isinstance(event_time, str):
        raise ValueError("Quote event_time must be an ISO timestamp.")

    try:
        datetime.fromisoformat(event_time)
    except ValueError as exc:
        raise ValueError("Quote event_time must be an ISO timestamp.") from exc

    raw_data = quote.get("raw_data")
    if (
        not isinstance(raw_data, list)
        or len(raw_data) < 8
        or raw_data[0] != "Q"
        or not isinstance(raw_data[1], str)
        or raw_data[1].upper() != symbol
        or not isinstance(raw_data[2], str)
    ):
        raise ValueError("Quote raw_data does not match its normalized fields.")

    try:
        raw_event_time = datetime.fromisoformat(raw_data[2])
    except ValueError as exc:
        raise ValueError("Quote raw_data timestamp must be ISO formatted.") from exc
    if raw_event_time != datetime.fromisoformat(event_time):
        raise ValueError("Quote raw_data timestamp does not match event_time.")

    parsed_quote = normalize_quote({"service": "fx", "data": raw_data})
    if parsed_quote["symbol"] != symbol or parsed_quote["event_time"] != event_time:
        raise ValueError("Quote raw_data does not match its normalized fields.")

    for field in ("bid_size", "bid_price", "mid_price", "ask_size", "ask_price"):
        if field in quote and quote[field] != parsed_quote[field]:
            raise ValueError(f"Quote {field} does not match raw_data.")
        quote[field] = parsed_quote[field]

    return quote


def commit_record(consumer, message):
    """Commit the next offset after a record has been handled."""
    partition = TopicPartition(message.topic, message.partition)
    consumer.commit({
        partition: OffsetAndMetadata(message.offset + 1, ""),
    })


def main():
    """Lê cotações do tópico Kafka até o utilizador interromper."""
    database = connect_database()
    consumer = None

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )

        for message in consumer:
            try:
                quote = validate_quote(message.value)
            except ValueError as exc:
                logger.warning(
                    "Mensagem inválida ignorada. Tópico: %s | Partição: %s | "
                    "Offset: %s | Motivo: %s",
                    message.topic,
                    message.partition,
                    message.offset,
                    exc,
                )
            else:
                inserted = save_quote(database, quote)
                if inserted:
                    logger.info(
                        "Cotação gravada no PostgreSQL: %s %s",
                        quote["symbol"],
                        quote["event_time"],
                    )
                else:
                    logger.info(
                        "Cotação já existente no PostgreSQL; sem duplicar: "
                        "%s %s",
                        quote["symbol"],
                        quote["event_time"],
                    )

            commit_record(consumer, message)
    except KeyboardInterrupt:
        logger.info("Consumidor interrompido pelo utilizador.")
    finally:
        if consumer is not None:
            consumer.close()
        database.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    main()