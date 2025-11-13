import json
import asyncio
import os

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

import structlog
from logging_config import setup_logging

# ---- ЛОГИ ----
setup_logging("analytics-worker")
log = structlog.get_logger(service="analytics-worker")

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
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "kafka:9092"
        )
        self._mongo_client = AsyncIOMotorClient("mongodb://mongodb:27017")
        self._mongo_db = self._mongo_client["mydatabase"]
        self.collection = self._mongo_db["book_views"]

        self._consumer: aiokafka.AIOKafkaConsumer | None = None
        self._task: asyncio.Task | None = None
        self._stopping = False

        log.info(
            "analytics_worker_initialized",
            topic=self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
        )

    async def _save_event(self, payload: dict) -> None:
        await self.collection.insert_one(payload)

    async def _loop(self):
        log.info(
            "analytics_worker_subscribing",
            topic=self.topic,
        )

        try:
            consumer = self._consumer
            if consumer is None:
                log.error("analytics_worker_consumer_none_stopping_loop")
                return

            async for msg in consumer:
                if self._stopping:
                    log.info("analytics_worker_stopping_flag_set_break_loop")
                    break

                
                carrier = msg.headers or []
                ctx = extract(carrier, getter=kafka_headers_getter)

           
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
                        log.error(
                            "analytics_worker_error_decoding_message",
                            error=str(e),
                            topic=msg.topic,
                            partition=msg.partition,
                            offset=msg.offset,
                        )
                        span.record_exception(e)
                        span.set_attribute("error", True)
                        continue

                    log.info(
                        "analytics_worker_received_message",
                        payload=payload,
                        topic=msg.topic,
                        partition=msg.partition,
                        offset=msg.offset,
                    )

                    try:
                        await self._save_event(payload)
                        await consumer.commit()
                        log.info(
                            "analytics_worker_message_processed_and_committed",
                            topic=msg.topic,
                            partition=msg.partition,
                            offset=msg.offset,
                        )
                    except Exception as e:
                        log.error(
                            "analytics_worker_error_saving_event",
                            error=str(e),
                            payload=payload,
                        )
                        span.record_exception(e)
                        span.set_attribute("error", True)

        except asyncio.CancelledError:
            log.info("analytics_worker_loop_cancelled")
        except Exception as e:
            log.error(
                "analytics_worker_consumer_loop_error",
                error=str(e),
            )

    async def start(self):
        if self._consumer is not None:
            log.warning("analytics_worker_start_called_but_consumer_already_exists")
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
                    log.info(
                        "analytics_worker_started_successfully",
                        attempt=attempt + 1,
                    )
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        log.warning(
                            "analytics_worker_connection_attempt_failed_retrying",
                            attempt=attempt + 1,
                            max_retries=max_retries,
                            error=str(e),
                            retry_delay=retry_delay,
                        )
                        await asyncio.sleep(retry_delay)
                    else:
                        log.error(
                            "analytics_worker_failed_to_connect_after_max_retries",
                            max_retries=max_retries,
                            error=str(e),
                        )
                        self._consumer = None
                        return

            if self._consumer is not None:
                self._task = asyncio.create_task(self._loop())
                log.info("analytics_worker_loop_task_started")

        except Exception as e:
            log.error(
                "analytics_worker_error_during_startup",
                error=str(e),
            )
            self._consumer = None

    async def stop(self):
        self._stopping = True
        log.info("analytics_worker_stop_requested")

        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                log.info("analytics_worker_task_cancelled")
            self._task = None

        if self._consumer is not None:
            await self._consumer.stop()
            log.info("analytics_worker_consumer_stopped")
            self._consumer = None

      
        self._mongo_client.close()
        log.info("analytics_worker_mongo_client_closed")

        log.info("analytics_worker_stopped")