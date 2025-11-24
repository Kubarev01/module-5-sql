import json
import os
from typing import Any, Dict, Optional

import structlog
from aiokafka import AIOKafkaProducer

from opentelemetry import trace
from opentelemetry.propagate import inject
from app.logging_config import setup_logging 

setup_logging("kafka-client")
log = structlog.get_logger()

tracer = trace.get_tracer(__name__)

producer: Optional[AIOKafkaProducer] = None


async def init_kafka_producer() -> None:
    """
    Вызываем один раз при старте сервиса.
    """
    global producer

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap_servers:
        raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS is not set")

    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        client_id="book-service-producer",
    )
    await producer.start()
    log.info("kafka_producer_started", bootstrap_servers=bootstrap_servers)


async def close_kafka_producer() -> None:
    """
    Вызываем при завершении сервиса.
    """
    global producer
    if producer is not None:
        await producer.stop()
        log.info("kafka_producer_stopped")
        producer = None


async def send_book_event(payload: dict, topic: str = "book_views") -> None:
    """
    Отправка события о книге в Kafka с прокидыванием trace-контекста в headers.
    """
    if producer is None:
        raise RuntimeError("Kafka producer is not initialized")

    value_bytes = json.dumps(payload).encode("utf-8")

    with tracer.start_as_current_span("kafka_send_book_event") as span:
        span.set_attribute("messaging.system", "kafka")
        span.set_attribute("messaging.destination", topic)

        carrier: Dict[str, str] = {}
        inject(carrier)

        headers = [(k, v.encode("utf-8")) for k, v in carrier.items()]

        try:
            await producer.send_and_wait(
                topic,
                value=value_bytes,
                headers=headers,
            )
            log.info(
                "kafka_message_sent",
                topic=topic,
                payload=payload,
            )
        except Exception as exc:
            log.warning(
                "kafka_message_send_failed",
                topic=topic,
                error=str(exc),
            )
            raise


async def send_message(topic: str, value: dict | list | str | bytes) -> None:
    """
    Универсальный хелпер для отправки произвольного сообщения без trace-контекста.
    """
    if producer is None:
        raise RuntimeError("Kafka producer is not initialized")

    if isinstance(value, (dict, list)):
        value_bytes = json.dumps(value).encode("utf-8")
    elif isinstance(value, str):
        value_bytes = value.encode("utf-8")
    else:
        value_bytes = value

    try:
        await producer.send_and_wait(topic, value=value_bytes)
        log.info("kafka_message_sent", topic=topic)
    except Exception as exc:
        log.warning(
            "kafka_message_send_failed",
            topic=topic,
            error=str(exc),
        )
        raise
