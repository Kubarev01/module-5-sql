from fastapi import FastAPI, Depends

from routers.books import router as book_router
from routers.review import router as review_router
from routers.common import router as common_router

from services.background_service import cache_invalidator
import asyncio

app = FastAPI()

# Dependency

app.include_router(book_router)
app.include_router(review_router)
app.include_router(common_router)

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.on_event("startup")
async def startup_event():
  
    asyncio.create_task(cache_invalidator())