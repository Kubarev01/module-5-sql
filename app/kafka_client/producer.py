import json
from typing import Any, Dict

from confluent_kafka import Producer

from opentelemetry import trace
from opentelemetry.propagate import inject

tracer = trace.get_tracer(__name__)


KAFKA_CONFIG: Dict[str, Any] = {
    "bootstrap.servers": "kafka:9092",
    "client.id": "book-service-producer",
}

producer = Producer(KAFKA_CONFIG)


def _delivery_report(err, msg) -> None:
    """
    Callback от Kafka — логируем успешную/неуспешную доставку.
    """
    if err is not None:
        print(f"[KafkaProducer] Delivery failed for {msg.topic()} [{msg.partition()}]: {err}")
    else:
        print(
            f"[KafkaProducer] Message delivered to {msg.topic()} "
            f"[{msg.partition()}] at offset {msg.offset()}"
        )


def send_book_event(payload: dict, topic: str = "book_views") -> None:
    """
    Отправка события о книге в Kafka с прокидыванием trace-контекста в headers.
    Вызывается из book-service (например, при создании/просмотре книги).
    """
    value_bytes = json.dumps(payload).encode("utf-8")

   
    with tracer.start_as_current_span("kafka_send_book_event") as span:
        span.set_attribute("messaging.system", "kafka")
        span.set_attribute("messaging.destination", topic)

        
        carrier: Dict[str, str] = {}
        inject(carrier)

        headers = [(k, v.encode("utf-8")) for k, v in carrier.items()]

        producer.produce(
            topic=topic,
            value=value_bytes,
            headers=headers,
            callback=_delivery_report,
        )


        producer.flush()


def send_message(topic: str, value: dict | list | str | bytes) -> None:
    """
    Универсальный хелпер для отправки произвольного сообщения без trace-контекста.
    """
    if isinstance(value, (dict, list)):
        value_bytes = json.dumps(value).encode("utf-8")
    elif isinstance(value, str):
        value_bytes = value.encode("utf-8")
    else:
        value_bytes = value

    producer.produce(topic=topic, value=value_bytes, callback=_delivery_report)
    producer.flush()