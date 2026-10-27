
"""Produtor de cotações Forex em tempo real da Tiingo."""

import json
import logging
import threading
import time
from datetime import datetime, timezone

import websocket
from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.serializer import JsonSerializer

from src.config import FOREX_SYMBOL, TIINGO_API_TOKEN
from src.normalize_quote import normalize_quote

WS_URL = "wss://api.tiingo.com/fx"
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
KAFKA_TOPIC = "forex-quotes"
INITIAL_RECONNECT_DELAY_SECONDS = 1
MAX_RECONNECT_DELAY_SECONDS = 60
STOP_EVENT = threading.Event()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def normalize_message(message: str) -> dict:
    """Descodifica e normaliza uma mensagem recebida da Tiingo."""
    received_at = datetime.now(timezone.utc).isoformat()

    try:
        payload = json.loads(message)
    except json.JSONDecodeError:
        logger.warning("Mensagem recebida não está em formato JSON.")
        return {
            "source": "tiingo",
            "symbol": FOREX_SYMBOL.upper(),
            "received_at": received_at,
            "raw_message": message,
            "parse_error": True,
        }

    if not isinstance(payload, dict):
        logger.warning("A mensagem JSON não é um objecto.")
        return {
            "source": "tiingo",
            "symbol": FOREX_SYMBOL.upper(),
            "received_at": received_at,
            "raw_data": payload,
            "parse_error": True,
        }

    data = payload.get("data")

    # Ignorar mensagens informativas que não sejam cotações.
        # Ignorar mensagens de outros serviços ou tipos informativos.
    if (
        payload.get("service") != "fx"
        or payload.get("messageType") != "A"
    ):
        logger.debug("Mensagem informativa recebida; não é uma cotação.")
        return {}

    # As mensagens do serviço FX devem conter uma lista de dados.
    # Mensagens malformadas ou que não sejam cotações serão validadas
    # por normalize_quote(), que permite registar o erro.
    if not isinstance(data, list) or not data or data[0] != "Q":
        try:
            normalize_quote(payload)
        except (ValueError, TypeError, KeyError) as exc:
            logger.warning("Cotação inválida recebida: %s", exc)
            return {
                "source": "tiingo",
                "symbol": FOREX_SYMBOL.upper(),
                "received_at": received_at,
                "raw_data": payload,
                "normalization_error": str(exc),
            }
        return {}

    try:
        normalized = normalize_quote(payload)
    except (ValueError, TypeError, KeyError) as exc:
        logger.warning("Cotação inválida recebida: %s", exc)
        return {
            "source": "tiingo",
            "symbol": FOREX_SYMBOL.upper(),
            "received_at": received_at,
            "raw_data": payload,
            "normalization_error": str(exc),
        }

    normalized["received_at"] = received_at
    return normalized


def on_open(ws):
    """Subscreve o par cambial após estabelecer a ligação."""
    subscription = {
        "eventName": "subscribe",
        "authorization": TIINGO_API_TOKEN,
        "eventData": {
            "thresholdLevel": 5,
            "tickers": [FOREX_SYMBOL.lower()],
        },
    }

    ws.send(json.dumps(subscription))
    logger.info("Subscrição enviada para %s.", FOREX_SYMBOL)


def _log_publish_success(record_metadata):
    logger.info(
        "Cotação publicada no Kafka. Tópico: %s | Partição: %s | Offset: %s",
        record_metadata.topic,
        record_metadata.partition,
        record_metadata.offset,
    )


def _log_publish_error(error):
    logger.error("Falha ao publicar cotação no Kafka: %s", error)


def on_message(ws, message, producer):
    """Normaliza as mensagens recebidas e publica cotações válidas no Kafka."""
    result = normalize_message(message)

    if not result:
        return

    if "normalization_error" in result or result.get("parse_error"):
        return

    try:
        future = producer.send(
            KAFKA_TOPIC,
            key=f"{result['symbol']}:{result['event_time']}".encode("utf-8"),
            value=result,
        )
    except KafkaError:
        logger.exception("Não foi possível enfileirar a cotação no Kafka.")
        return

    future.add_callback(_log_publish_success)
    future.add_errback(_log_publish_error)
    logger.debug("Cotação enviada para publicação no Kafka: %s", result)


def on_error(ws, error):
    """Regista erros sem expor credenciais."""
    if isinstance(error, KeyboardInterrupt):
        STOP_EVENT.set()
        logger.info("Interrupção recebida; a terminar o produtor.")
        return

    error_details = repr(error)
    if TIINGO_API_TOKEN:
        error_details = error_details.replace(TIINGO_API_TOKEN, "[REDACTED]")

    logger.error(
        "Erro na ligação WebSocket (%s): %s",
        type(error).__name__,
        error_details,
    )


def on_close(ws, status_code, message):
    """Regista o encerramento da ligação."""
    logger.info(
        "Ligação encerrada. Código: %s | Motivo: %s",
        status_code,
        message,
    )


def main():
    """Inicia o cliente WebSocket da Tiingo."""
    retry_delay = INITIAL_RECONNECT_DELAY_SECONDS
    STOP_EVENT.clear()
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=JsonSerializer(),
        enable_idempotence=True,
        acks="all",
        retries=5,
    )

    try:
        while True:
            client = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=lambda ws, message: on_message(
                    ws,
                    message,
                    producer,
                ),
                on_error=on_error,
                on_close=on_close,
            )

            logger.info("A iniciar ligação WebSocket para %s.", FOREX_SYMBOL)
            try:
                client.run_forever(ping_interval=30, ping_timeout=10)
            except KeyboardInterrupt:
                STOP_EVENT.set()
                logger.info("Produtor interrompido pelo utilizador.")
                break
            except Exception as exc:  # pragma: no cover - fallback da reconexão
                logger.exception(
                    "Erro inesperado na ligação WebSocket: %s",
                    type(exc).__name__,
                )

            if STOP_EVENT.is_set():
                break

            logger.warning(
                "Ligação terminada; nova tentativa em %s segundos.",
                retry_delay,
            )
            time.sleep(retry_delay)
            retry_delay = min(
                retry_delay * 2,
                MAX_RECONNECT_DELAY_SECONDS,
            )
    except KeyboardInterrupt:
        logger.info("Produtor interrompido pelo utilizador.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
