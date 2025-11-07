import os
import json
import threading
from confluent_kafka import Consumer, KafkaException, KafkaError



def create_consumer() -> Consumer:
    config = {

        "bootstrap.servers": "kafka:9092",
        "group.id": "analytics",
        "auto.offset.reset": "earliest", 
        "enable.auto.commit": True,
    }

    return Consumer(config)

class AnaliticsWorker:
    def __init__(self, topic: str = "book_views"):
        self.consumer = create_consumer()
        self.topic = topic
        self._stop_flag = False
        self._thread: threading.Thread|None = None

    def _loop(self):
        print(f"[AnalyticsWorker] Subscribing to topic: {self.topic}")
        self.consumer.subscribe([self.topic])
        try:
            while not self._stop_flag:
                msg = self.consumer.poll(1.0) # ждем 1 сек
                if msg is None: # если нет сообщений
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF: # не ошибка а конец партиции
                        print(f"{msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}")
                    elif msg.error():
                        print(f"ERROR: {msg.error()}")
                else:
                    print(f"Consumed record with key {msg.key()} and value {msg.value()}")
        except KafkaException as e:
            print(f"[AnalyticsWorker] KafkaException: {e}")
        finally:
            print("[AnalyticsWorker] Closing consumer...")
            self.consumer.close()

    def start(self):
        if self._thread is None:
            self._stop_flag = False
            self._thread = threading.Thread(target=self._loop) 
            self._thread.start()
            print("[AnalyticsWorker] Started")

    def stop(self):
        if self._thread is not None:
            self._stop_flag = True
            self._thread.join(timeout=5)
            self._thread = None
            print("[AnalyticsWorker] Stopped")