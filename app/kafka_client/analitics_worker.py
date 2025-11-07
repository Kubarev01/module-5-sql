import os
import json
import threading
import asyncio
from confluent_kafka import Consumer, KafkaException, KafkaError
from motor.motor_asyncio import AsyncIOMotorClient
from database.mongo_client import db
from pymongo import MongoClient


MONGO_URI = os.getenv("MONGO_URL", "mongodb://mongodb:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "mydatabase")





def create_consumer() -> Consumer:
    config = {

        "bootstrap.servers": "kafka:9092",
        "group.id": "analytics",
        "auto.offset.reset": "earliest", 
        "enable.auto.commit": False,
    }

    return Consumer(config)

class AnaliticsWorker:

    #синхронный для аналитики
    _mongo_client = MongoClient(MONGO_URI)
    _mongo_db = _mongo_client[MONGO_DB_NAME]


    def __init__(self, topic: str = "book_views"):
        self.consumer = create_consumer()
        self.topic = topic
        self._stop_flag = False
        self._thread: threading.Thread| None = None
        self.mongo_db = db
        self.collection = self._mongo_db["book_views"]
    
    def _save_event(self, payload: dict) -> None:
       # сохраняем событие в mongo
       self.collection.insert_one(payload)

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
                   
                    value_bytes = msg.value()
                    try:
                        payload = json.loads(value_bytes.decode("utf-8"))
                    except Exception as e:
                        print(f"[AnalyticsWorker] Error decoding message: {e}, raw message: {value_bytes}")
                        continue
                    print(f"Consumed record with key {msg.key()} and value {msg.value()}")

                    try:
                        self._save_event(payload)
                        self.consumer.commit(message=msg, asynchronous=True)
                    except Exception as e:
                        print(f"[AnalyticsWorker] Error saving event to MongoDB: {e}")

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