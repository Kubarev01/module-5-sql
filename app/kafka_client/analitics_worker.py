import json
import asyncio
import aiokafka
from motor.motor_asyncio import AsyncIOMotorClient


class AnaliticsWorker:
    def __init__(
        self,
        topic: str = "book_views",
        bootstrap_servers: str | None = None,
        group_id: str = "analytics",
    ):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers or "kafka:9092"
        self.group_id = group_id

        # MongoDB клиент
        self._mongo_client = AsyncIOMotorClient("mongodb://mongodb:27017")
        self._mongo_db = self._mongo_client["mydatabase"]
        self.collection = self._mongo_db["book_views"]
        
        self._consumer: aiokafka.AIOKafkaConsumer | None = None
        self._task: asyncio.Task | None = None
        self._stopping = False

    async def _save_event(self, payload: dict) -> None:
        await self.collection.insert_one(payload)

    async def _loop(self):
        print(f"[AnalyticsWorker] Subscribing to topic: {self.topic}")

        try:
            # Сохраняем ссылку на consumer в начале
            consumer = self._consumer
            if consumer is None:
                print("[AnalyticsWorker] Consumer is None, stopping loop")
                return

            async for msg in consumer:
                # Проверяем флаг остановки
                if self._stopping:
                    break

                try:
                    payload = json.loads(msg.value.decode("utf-8"))
                except Exception as e:
                    print(f"[AnalyticsWorker] Error decoding message: {e}")
                    continue

                print(f"[AnalyticsWorker] Received message: {payload}")

                try:
                    await self._save_event(payload)
                    # Коммитим используя локальную переменную
                    await consumer.commit()
                except Exception as e:
                    print(f"[AnalyticsWorker] Error saving event: {e}")
                    
        except asyncio.CancelledError:
            print("[AnalyticsWorker] Loop cancelled")
        except Exception as e:
            print(f"[AnalyticsWorker] Consumer loop error: {e}")

    async def start(self):
        if self._consumer is not None:
            return

        self._stopping = False
        
        try:
            self._consumer = aiokafka.AIOKafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
            )

            # Увеличиваем количество попыток
            max_retries = 10
            retry_delay = 10
            
            for attempt in range(max_retries):
                try:
                    await self._consumer.start()
                    print("[AnalyticsWorker] Started successfully")
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        print(f"[AnalyticsWorker] Connection attempt {attempt + 1} failed, retrying...")
                        await asyncio.sleep(retry_delay)
                    else:
                        print(f"[AnalyticsWorker] Failed to connect after {max_retries} attempts")
                        self._consumer = None
                        return

            # Запускаем цикл только если consumer создан
            if self._consumer is not None:
                self._task = asyncio.create_task(self._loop())
                
        except Exception as e:
            print(f"[AnalyticsWorker] Error during startup: {e}")
            self._consumer = None

    async def stop(self):
        self._stopping = True

        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None

        print("[AnalyticsWorker] Stopped")