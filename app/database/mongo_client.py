import motor.motor_asyncio
import os

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.zipkin.json import ZipkinExporter
from opentelemetry.semconv.resource import ResourceAttributes

resource = Resource(
    attributes={
        ResourceAttributes.SERVICE_NAME: "mongodb",  # уникальное!
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
        print("✅ MongoDB подключен")
    except Exception as e:
        print("❌ Ошибка подключения к MongoDB:", e)

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_connection())