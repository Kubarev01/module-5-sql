import motor.motor_asyncio
import os
import structlog
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.zipkin.json import ZipkinExporter
from opentelemetry.semconv.resource import ResourceAttributes
from app.logging_config import setup_logging

#логи
setup_logging("mongo-client")
log = structlog.get_logger()

resource = Resource(
    attributes={
        ResourceAttributes.SERVICE_NAME: "mongodb",  
    }
)

provider = TracerProvider(resource=resource)
exporter = ZipkinExporter(endpoint="http://zipkin:9411/api/v2/spans")
provider.add_span_processor(BatchSpanProcessor(exporter))
trace.set_tracer_provider(provider)

tracer = trace.get_tracer(__name__)

MONGO_URL = os.getenv("MONGO_URL")

mongo_client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URL)

db = mongo_client["mydatabase"]

async def test_connection():
    try:
        await mongo_client.admin.command('ping')
        log.info("✅ MongoDB подключен")
    except Exception as e:
        log.info("❌ Ошибка подключения к MongoDB:", e)

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_connection())