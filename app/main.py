from fastapi import FastAPI, Depends, Response

from routers.books import router as book_router
from routers.review import router as review_router
from routers.common import router as common_router
from routers.inventory import router as inventory_router
from routers.authors import router as author_router
from routers.orders import router as order_router

from services.background_service import cache_invalidator
import asyncio
from prometheus_client import CollectorRegistry

from opentelemetry import trace, metrics
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.zipkin.json import ZipkinExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.semconv.resource import ResourceAttributes

from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from database.postgres_client import engine

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.exporter.prometheus import PrometheusMetricReader

from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from logging_config import setup_logging
import structlog

# ---- ЛОГИ ----
setup_logging("book-service")
log = structlog.get_logger(service="book-service")


SQLAlchemyInstrumentor().instrument(engine=engine.sync_engine)

resource = Resource(
    attributes={
        ResourceAttributes.SERVICE_NAME: "book-service",
    }
)

trace_provider = TracerProvider(resource=resource)
zipkin_exporter = ZipkinExporter(
    endpoint="http://zipkin:9411/api/v2/spans",
)
trace_provider.add_span_processor(BatchSpanProcessor(zipkin_exporter))
trace.set_tracer_provider(trace_provider)

metrics_resource = Resource(
    attributes={
        ResourceAttributes.SERVICE_NAME: "book-service",
    }
)

prom_reader = PrometheusMetricReader()

meter_provider = MeterProvider(
    resource=metrics_resource,
    metric_readers=[prom_reader],
)

metrics.set_meter_provider(meter_provider)

meter = metrics.get_meter_provider().get_meter("book-service")

book_created_counter = meter.create_counter(
    name="book_created_total",
    description="Total number of created books",
    unit="1",
)

# ---------------- FastAPI app ----------------
app = FastAPI()

# Инструментируем FastAPI и httpx для трейсов
FastAPIInstrumentor.instrument_app(app)
HTTPXClientInstrumentor().instrument()

# Роутеры
app.include_router(book_router)
app.include_router(review_router)
app.include_router(author_router)
app.include_router(common_router)
app.include_router(inventory_router)
app.include_router(order_router)


@app.get("/")
async def root():
    return {"message": "Hello World"}
@app.get("/health")
def health():
    return {"status": "ok", "service": "book-service"}

@app.get("/healthz", include_in_schema=False)
async def healthz():
    return {"status": "ok", "service": "book-service"}  

registry = CollectorRegistry()

# --- /metrics для Prometheus ---
@app.get("/metrics")
def metrics_endpoint() -> Response:
    """
    Эндпоинт, который скрейпает Prometheus.
    Prometheus ходит сюда: book-service:8000/metrics
    """
    return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(cache_invalidator())