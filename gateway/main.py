from fastapi import Depends, FastAPI, HTTPException, Response
import httpx
import os

from pydantic import BaseModel
from auth import verify_token

# ==== OpenTelemetry Traces (Zipkin) ====
from opentelemetry import trace, metrics
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.exporter.zipkin.json import ZipkinExporter
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    SimpleSpanProcessor,
    ConsoleSpanExporter,
)
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

# ==== OpenTelemetry Metrics (Prometheus) ====
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

# ==== PyBreaker ====
from pybreaker import (
    CircuitBreaker,
    CircuitBreakerListener,
    CircuitBreakerError,
    STATE_CLOSED,
    STATE_HALF_OPEN,
    STATE_OPEN,
)


# -------------------------------------------------------------------
#   OTEL: TRACES
# -------------------------------------------------------------------
resource = Resource(
    attributes={
        ResourceAttributes.SERVICE_NAME: "gateway",
    }
)

trace_provider = TracerProvider(resource=resource)

zipkin_exporter = ZipkinExporter(
    endpoint="http://zipkin:9411/api/v2/spans",
)

trace_provider.add_span_processor(BatchSpanProcessor(zipkin_exporter))
trace_provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))

trace.set_tracer_provider(trace_provider)
tracer = trace.get_tracer(__name__)


# -------------------------------------------------------------------
#   OTEL: METRICS (Prometheus)
# -------------------------------------------------------------------
prom_reader = PrometheusMetricReader()
meter_provider = MeterProvider(metric_readers=[prom_reader])
metrics.set_meter_provider(meter_provider)

# общий meter для gateway
meter = metrics.get_meter("gateway")

# Пример технических метрик (не обязательно, но пусть будут)
request_counter = meter.create_counter(
    name="gateway_requests_total",
    description="Total number of requests to gateway",
)

request_duration_hist = meter.create_histogram(
    name="gateway_request_duration_ms",
    description="Duration of gateway requests in milliseconds",
)


# -------------------------------------------------------------------
#   PyBreaker + метрика circuit_breaker_state
# -------------------------------------------------------------------

# Здесь храним состояние всех предохранителей:
# 0 = CLOSED, 1 = HALF_OPEN, 2 = OPEN
_circuit_breaker_states: dict[str, int] = {}


class OtelCircuitBreakerListener(CircuitBreakerListener):
    STATE_MAP = {
        STATE_CLOSED: 0,
        STATE_HALF_OPEN: 1,
        STATE_OPEN: 2,
    }

    def state_change(self, cb, old_state, new_state):
        # cb.name — имя breaker'а
        new_val = self.STATE_MAP.get(new_state, 0)
        _circuit_breaker_states[cb.name] = new_val


otel_cb_listener = OtelCircuitBreakerListener()

# Сам предохранитель для book-service
book_service_breaker = CircuitBreaker(
    fail_max=3,             # после 3 ошибок — OPEN
    reset_timeout=30,       # через 30 сек перейдёт в HALF_OPEN
    name="book_service_breaker",
    listeners=[otel_cb_listener],
)

# Инициализируем состояние в "закрыт", чтобы метрика появилась сразу
_circuit_breaker_states[book_service_breaker.name] = 0


def _cb_state_callback(options: metrics.CallbackOptions):
    """
    Observable gauge callback: возвращает Observation по всем breaker'ам.
    """
    observations: list[metrics.Observation] = []
    for name, state in _circuit_breaker_states.items():
        observations.append(
            metrics.Observation(
                value=state,
                attributes={"breaker_name": name},
            )
        )
    return observations


circuit_breaker_state_gauge = meter.create_observable_gauge(
    name="circuit_breaker_state",
    callbacks=[_cb_state_callback],
    description="Circuit breaker state: 0=closed, 1=half-open, 2=open",
    unit="1",
)


# -------------------------------------------------------------------
#   FastAPI app + OTEL-инструментация
# -------------------------------------------------------------------
app = FastAPI(title="gateway", version="0.0.1")

FastAPIInstrumentor.instrument_app(app)
HTTPXClientInstrumentor().instrument()

BOOK_SERVICE_URL = os.getenv("BOOK_SERVICE_URL", "http://book-service:8000")


# -------------------------------------------------------------------
#   /metrics — endpoint для Prometheus
# -------------------------------------------------------------------
@app.get("/metrics")
async def metrics_endpoint():
    # prom_reader._collector — внутренний CollectRegistry,
    # generate_latest умеет его читать
    data = generate_latest(prom_reader._collector)
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)


# -------------------------------------------------------------------
#   Health
# -------------------------------------------------------------------
@app.get("/health")
async def health():
    return {"status": "ok", "service": "gateway"}


# -------------------------------------------------------------------
#   Схемы
# -------------------------------------------------------------------
class BaseSchema(BaseModel):
    pass


class BookSchema(BaseSchema):
    title: str
    genre: str
    author_id: int | None = None


# -------------------------------------------------------------------
#   Хелпер для запросов в book-service через CircuitBreaker
# -------------------------------------------------------------------
async def _call_book_service_get(path: str):
    """
    Обёртка GET-запроса к book-service, которая прогоняет результат
    через CircuitBreaker, чтобы тот мог менять своё состояние.
    """
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{BOOK_SERVICE_URL}{path}")

    # Здесь мы уже получили resp, теперь дадим breaker'у
    # решить — success или failure. Для этого оборачиваем
    # проверку в синхронную функцию, которую передаём в call().
    def check_response():
        resp.raise_for_status()
        return resp

    # если raise_for_status() бросит исключение — breaker увидит ошибку
    resp = book_service_breaker.call(check_response)
    return resp


async def _call_book_service_post(path: str, json_body: dict):
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{BOOK_SERVICE_URL}{path}", json=json_body)

    def check_response():
        resp.raise_for_status()
        return resp

    resp = book_service_breaker.call(check_response)
    return resp


# -------------------------------------------------------------------
#   Роуты
# -------------------------------------------------------------------
@app.get("/books/{book_id}")
async def get_book(book_id: int, user=Depends(verify_token)):
    try:
        resp = await _call_book_service_get(f"/books/{book_id}")
    except CircuitBreakerError:
        # предохранитель в OPEN/ HALF_OPEN и не даёт ходить в book-service
        raise HTTPException(
            status_code=503,
            detail="book-service temporarily unavailable (circuit breaker open)",
        )
    except httpx.HTTPError as e:
        # реальная ошибка book-service
        raise HTTPException(status_code=502, detail=f"book-service error: {e!r}")

    return resp.json()


@app.get("/{book_id}/details")
async def get_detail_book(book_id: int, user=Depends(verify_token)):
    try:
        resp = await _call_book_service_get(f"/{book_id}/details/")
    except CircuitBreakerError:
        raise HTTPException(
            status_code=503,
            detail="book-service temporarily unavailable (circuit breaker open)",
        )
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"book-service error: {e!r}")

    return resp.json()


@app.post("/books")
async def create_book(book_schema: BookSchema, user=Depends(verify_token)):
    try:
        resp = await _call_book_service_post("/books/", book_schema.dict())
    except CircuitBreakerError:
        raise HTTPException(
            status_code=503,
            detail="book-service temporarily unavailable (circuit breaker open)",
        )
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"book-service error: {e!r}")

    return resp.json()