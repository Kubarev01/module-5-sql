import time
from typing import Dict

# OTEL: traces + metrics
from opentelemetry import trace, metrics
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.resource import ResourceAttributes

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    SimpleSpanProcessor,
    ConsoleSpanExporter,
)
from opentelemetry.exporter.zipkin.json import ZipkinExporter

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import Observation

# pybreaker
from pybreaker import (
    CircuitBreaker,
    CircuitBreakerListener,
    STATE_CLOSED,
    STATE_HALF_OPEN,
    STATE_OPEN,
)

# =========================
#  OTEL: Resource
# =========================
resource = Resource(
    attributes={
        ResourceAttributes.SERVICE_NAME: "gateway",
    }
)

# =========================
#  METRICS: Prometheus + MeterProvider
# =========================
prom_reader = PrometheusMetricReader()

meter_provider = MeterProvider(
    metric_readers=[prom_reader],
    resource=resource,
)
metrics.set_meter_provider(meter_provider)

meter = metrics.get_meter_provider().get_meter("gateway")

# Счётчик запросов и гистограмма длительности
request_counter = meter.create_counter(
    name="gateway_requests_total",
    description="Total number of requests to gateway",
)

request_duration_hist = meter.create_histogram(
    name="gateway_request_duration_ms",
    description="Duration of gateway requests in milliseconds",
    unit="ms",
)

# =========================
#  Circuit Breaker + метрика состояния
# =========================

# Сюда будем писать текущее состояние каждого breaker'а:
# 0 = closed, 1 = half-open, 2 = open
_circuit_breaker_states: Dict[str, int] = {}


class OtelCircuitBreakerListener(CircuitBreakerListener):
    """
    Listener для pybreaker, который при смене состояния
    обновляет словарь _circuit_breaker_states.
    """

    STATE_MAP = {
        STATE_CLOSED: 0,
        STATE_HALF_OPEN: 1,
        STATE_OPEN: 2,
    }

    def state_change(self, cb, old_state, new_state):
        new_val = self.STATE_MAP.get(new_state, 0)
        _circuit_breaker_states[cb.name] = new_val


def _cb_state_callback(options):
    """
    Observable gauge callback: возвращает Observation по каждому breaker'у.
    """
    for name, state in _circuit_breaker_states.items():
        yield Observation(
            value=state,
            attributes={"breaker_name": name},
        )


circuit_breaker_state_gauge = meter.create_observable_gauge(
    name="circuit_breaker_state",
    callbacks=[_cb_state_callback],
    description="Circuit breaker state: 0=closed, 1=half-open, 2=open",
    unit="1",
)

# Основной CircuitBreaker для book-service
gateway_cb_listener = OtelCircuitBreakerListener()

book_service_breaker = CircuitBreaker(
    fail_max=5,              # после 5 ошибок откроется
    reset_timeout=30,        # через 30 секунд попробует half-open
    listeners=[gateway_cb_listener],
    name="book_service_breaker",
)

# =========================
#  TRACING: Zipkin
# =========================
trace_provider = TracerProvider(resource=resource)

zipkin_exporter = ZipkinExporter(
    endpoint="http://zipkin:9411/api/v2/spans",
)

trace_provider.add_span_processor(BatchSpanProcessor(zipkin_exporter))
trace_provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))

trace.set_tracer_provider(trace_provider)
tracer = trace.get_tracer(__name__)