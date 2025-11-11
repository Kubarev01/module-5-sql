from fastapi import Depends, FastAPI, HTTPException
import httpx
import os
from auth import verify_token
from pydantic import BaseModel
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.exporter.zipkin.json import ZipkinExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    SimpleSpanProcessor,
    ConsoleSpanExporter,
)

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


app = FastAPI(title="gateway", version="0.0.1")

FastAPIInstrumentor.instrument_app(app)
HTTPXClientInstrumentor().instrument()

BOOK_SERVICE_URL = os.getenv("BOOK_SERVICE_URL", "http://book-service")


@app.get("/health")
async def health():
    return {"status": "ok", "service": "gateway"}


@app.get("/books/{book_id}")
async def get_book(book_id: int, user=Depends(verify_token)):
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{BOOK_SERVICE_URL}/books/{book_id}")
        resp.raise_for_status()
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"book-service error: {e!r}")
    return resp.json()


@app.get("/{book_id}/details")
async def get_detail_book(book_id: int, user=Depends(verify_token)):
    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await client.get(f"{BOOK_SERVICE_URL}/{book_id}/details/")
        resp.raise_for_status()
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"book-service error: {e!r}")
    return resp.json()


class BaseSchema(BaseModel):
    pass

class BookSchema(BaseSchema):

    title: str
    genre: str
    author_id: int | None = None


@app.post("/books")
async def create_book(book_schema: BookSchema, user=Depends(verify_token)):
    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await client.post(f"{BOOK_SERVICE_URL}/books/", json=book_schema.dict())
        resp.raise_for_status()
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"book-service error: {e!r}")
    return resp.json()
