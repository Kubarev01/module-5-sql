import json
import asyncio

import aiokafka
from motor.motor_asyncio import AsyncIOMotorClient

# ==== OpenTelemetry / Zipkin ====
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.zipkin.json import ZipkinExporter
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.propagate import extract
from opentelemetry.instrumentation.pymongo import PymongoInstrumentor


# --- Инициализация OTEL для analytics-worker ---
resource = Resource(
    attributes={
        ResourceAttributes.SERVICE_NAME: "analytics-worker",
    }
)

trace_provider = TracerProvider(resource=resource)
zipkin_exporter = ZipkinExporter(
    endpoint="http://zipkin:9411/api/v2/spans",  # внутри docker-сети
)
trace_provider.add_span_processor(BatchSpanProcessor(zipkin_exporter))
trace.set_tracer_provider(trace_provider)

tracer = trace.get_tracer(__name__)

# Инструментируем Mongo (Motor поверх pymongo)
PymongoInstrumentor().instrument()


# --- Helper для чтения заголовков из Kafka в OTEL ---
class KafkaHeadersGetter:
    """
    Адаптер под интерфейс Getter для opentelemetry.propagate.extract.
    carrier здесь — это msg.headers (список пар (key, value)).
    """

    def get(self, carrier, key):
        if not carrier:
            return None

        key = key.lower()
        values: list[str] = []

        for k, v in carrier:
            if isinstance(k, bytes):
                k = k.decode("utf-8")
            if k.lower() == key:
                if isinstance(v, bytes):
                    v = v.decode("utf-8")
                values.append(v)

        return values or None

    def keys(self, carrier):
        if not carrier:
            return []

        keys: list[str] = []
        for k, _ in carrier:
            if isinstance(k, bytes):
                k = k.decode("utf-8")
            keys.append(k)
        return keys


kafka_headers_getter = KafkaHeadersGetter()


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
            consumer = self._consumer
            if consumer is None:
                print("[AnalyticsWorker] Consumer is None, stopping loop")
                return

            async for msg in consumer:
                if self._stopping:
                    break

                # ==== Восстанавливаем trace-контекст из Kafka headers ====
                carrier = msg.headers or []
                ctx = extract(carrier, getter=kafka_headers_getter)

                # ==== Обработка события ====
                # Span вокруг обработки события
                with tracer.start_as_current_span(
                    "analytics_process_book_event",
                    context=ctx,
                ) as span:
                    span.set_attribute("kafka.topic", msg.topic)
                    span.set_attribute("kafka.partition", msg.partition)
                    span.set_attribute("kafka.offset", msg.offset)

                    try:
                        payload = json.loads(msg.value.decode("utf-8"))
                    except Exception as e:
                        print(f"[AnalyticsWorker] Error decoding message: {e}")
                        span.record_exception(e)
                        span.set_attribute("error", True)
                        continue

                    print(f"[AnalyticsWorker] Received message: {payload}")

                    try:
                        await self._save_event(payload)
                        # Явный commit после успешной обработки
                        await consumer.commit()
                    except Exception as e:
                        print(f"[AnalyticsWorker] Error saving event: {e}")
                        span.record_exception(e)
                        span.set_attribute("error", True)

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

            max_retries = 10
            retry_delay = 10

            for attempt in range(max_retries):
                try:
                    await self._consumer.start()
                    print("[AnalyticsWorker] Started successfully")
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        print(
                            f"[AnalyticsWorker] Connection attempt {attempt + 1} failed "
                            f"({e}), retrying..."
                        )
                        await asyncio.sleep(retry_delay)
                    else:
                        print(
                            f"[AnalyticsWorker] Failed to connect after {max_retries} attempts: {e}"
                        )
                        self._consumer = None
                        return

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

        # Закрываем Mongo-клиент
        self._mongo_client.close()

        print("[AnalyticsWorker] Stopped")