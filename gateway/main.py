import os
import time

import httpx
from fastapi import Depends, FastAPI, HTTPException, Response
from starlette.concurrency import run_in_threadpool
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest, REGISTRY

from auth import verify_token

from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from pybreaker import CircuitBreakerError

from observability import (
    tracer,
    prom_reader,
    request_counter,
    request_duration_hist,
    book_service_breaker,
)
from schemas import BookSchema


app = FastAPI(title="gateway", version="0.0.1")

FastAPIInstrumentor.instrument_app(app)
HTTPXClientInstrumentor().instrument()

BOOK_SERVICE_URL = os.getenv("BOOK_SERVICE_URL", "http://book-service:8000")


# -------------------------
#  СИНХРОННЫЕ функции для pybreaker
# -------------------------


def _sync_get_book(book_id: int) -> httpx.Response:
    resp = httpx.get(f"{BOOK_SERVICE_URL}/books/{book_id}")
    resp.raise_for_status()
    return resp


def _sync_get_book_details(book_id: int) -> httpx.Response:
    resp = httpx.get(f"{BOOK_SERVICE_URL}/{book_id}/details/")
    resp.raise_for_status()
    return resp


def _sync_create_book(payload: dict) -> httpx.Response:
    resp = httpx.post(f"{BOOK_SERVICE_URL}/books/", json=payload)
    resp.raise_for_status()
    return resp


# =========================
#  ENDPOINTS
# =========================


@app.get("/health")
async def health():
    return {"status": "ok", "service": "gateway"}

@app.get("/healthz")
def healthz():
    return {"status": "ok"}

@app.get("/books/{book_id}")
async def get_book(book_id: int, user=Depends(verify_token)):
    start = time.perf_counter()
    attrs = {"endpoint": "/books/{book_id}", "method": "GET"}

    try:
        resp: httpx.Response = await run_in_threadpool(
            book_service_breaker.call,
            _sync_get_book,
            book_id,
        )
    except CircuitBreakerError as e:
        request_counter.add(1, attributes={**attrs, "cb_state": "open"})
        raise HTTPException(
            status_code=503,
            detail=f"circuit breaker open for book-service: {e!r}",
        )
    except httpx.HTTPError as e:
        request_counter.add(1, attributes={**attrs, "cb_state": "closed"})
        raise HTTPException(status_code=502, detail=f"book-service error: {e!r}")
    finally:
        duration_ms = (time.perf_counter() - start) * 1000
        request_duration_hist.record(duration_ms, attributes=attrs)

    return resp.json()


@app.get("/{book_id}/details")
async def get_detail_book(book_id: int, user=Depends(verify_token)):
    start = time.perf_counter()
    attrs = {"endpoint": "/{book_id}/details", "method": "GET"}

    try:
        resp: httpx.Response = await run_in_threadpool(
            book_service_breaker.call,
            _sync_get_book_details,
            book_id,
        )
    except CircuitBreakerError as e:
        request_counter.add(1, attributes={**attrs, "cb_state": "open"})
        raise HTTPException(
            status_code=503,
            detail=f"circuit breaker open for book-service: {e!r}",
        )
    except httpx.HTTPError as e:
        request_counter.add(1, attributes={**attrs, "cb_state": "closed"})
        raise HTTPException(status_code=502, detail=f"book-service error: {e!r}")
    finally:
        duration_ms = (time.perf_counter() - start) * 1000
        request_duration_hist.record(duration_ms, attributes=attrs)

    return resp.json()


@app.post("/books")
async def create_book(book_schema: BookSchema, user=Depends(verify_token)):
    start = time.perf_counter()
    attrs = {"endpoint": "/books", "method": "POST"}

    try:
        resp: httpx.Response = await run_in_threadpool(
            book_service_breaker.call,
            _sync_create_book,
            book_schema.dict(),
        )
    except CircuitBreakerError as e:
        request_counter.add(1, attributes={**attrs, "cb_state": "open"})
        raise HTTPException(
            status_code=503,
            detail=f"circuit breaker open for book-service: {e!r}",
        )
    except httpx.HTTPError as e:
        request_counter.add(1, attributes={**attrs, "cb_state": "closed"})
        raise HTTPException(status_code=502, detail=f"book-service error: {e!r}")
    finally:
        duration_ms = (time.perf_counter() - start) * 1000
        request_duration_hist.record(duration_ms, attributes=attrs)

    return resp.json()


@app.get("/metrics")
async def metrics_endpoint():
    """
    Прометеус будет скрейпить этот эндпоинт.
    """
    return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)