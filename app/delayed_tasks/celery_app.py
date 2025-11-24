from celery import Celery

from opentelemetry.instrumentation.celery import CeleryInstrumentor
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.zipkin.json import ZipkinExporter
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.instrumentation.pymongo import PymongoInstrumentor


BROKER_URL = "amqp://guest:guest@rabbbitmq:5672//"

RESULT_BACKEND = "rpc://"

resource = Resource(
    attributes={ResourceAttributes.SERVICE_NAME: "analytics-worker"}
)

provider = TracerProvider(resource=resource)
exporter = ZipkinExporter(endpoint="http://zipkin:9411/api/v2/spans")
provider.add_span_processor(BatchSpanProcessor(exporter))
trace.set_tracer_provider(provider)

tracer = trace.get_tracer(__name__)

# Подсветить Mongo
PymongoInstrumentor().instrument()

celery_app = Celery(
    "my_app",  
    broker=BROKER_URL,
    backend=RESULT_BACKEND,

)

CeleryInstrumentor().instrument()



celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_acks_late=True,     
    task_reject_on_worker_lost=True,
    timezone="UTC",
    enable_utc=True
)

celery_app.conf.beat_schedule = {
    "nightly-report-every-5-min":{
        "task": "delayed_tasks.worker_service.nightly_report",
        "schedule": 5*60,
        "args": (),
    }
}