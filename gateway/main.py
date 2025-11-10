from fastapi import Depends, FastAPI, HTTPException
import httpx
import os

from auth import verify_token

app = FastAPI(title="API Gateway")

BOOK_SERVICE_URL = "http://book-service:8000"


@app.get("/health")
async def health():
    return {"status": "ok", "service": "gateway"}

@app.get("/books/{book_id}")
async def get_book(book_id: int, user = Depends(verify_token)):
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{BOOK_SERVICE_URL}/books/{book_id}")
        resp.raise_for_status()
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"book-service error: {e!r}")
    return resp.json()

@app.get("/{book_id}/details")
async def get_detail_book(book_id:int , user = Depends(verify_token)):
    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await client.get(f"{BOOK_SERVICE_URL}/{book_id}/details/")
        resp.raise_for_status()
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"book-service error: {e!r}")
    return resp.json()