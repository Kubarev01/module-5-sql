from fastapi import FastAPI, Depends

from routers.books import router as book_router
from routers.review import router as review_router
from routers.common import router as common_router
from routers.inventory import router as inventory_router
from routers.authors import router as author_router
from routers.orders import router as order_router

from services.background_service import cache_invalidator
import asyncio

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.zipkin.json import ZipkinExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.semconv.resource import ResourceAttributes

from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from database.postgres_client import engine 

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


app = FastAPI()


FastAPIInstrumentor.instrument_app(app)
HTTPXClientInstrumentor().instrument()
# Dependency

app.include_router(book_router)
app.include_router(review_router)
app.include_router(author_router)
app.include_router(common_router)
app.include_router(inventory_router)
app.include_router(order_router)


@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(cache_invalidator())

