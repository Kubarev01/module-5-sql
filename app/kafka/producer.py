from confluent_kafka import Producer


config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
}

producer = Producer(config)


def send_message(topic, key, value):
    producer.produce(topic=topic, value=value)
    producer.flush()