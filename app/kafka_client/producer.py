from confluent_kafka import Producer
import json

config = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
}

producer = Producer(config)


def send_message(topic: str, value):
    payload = json.dumps(value).encode("utf-8")
    producer.produce(topic=topic, value=payload)
    producer.flush()