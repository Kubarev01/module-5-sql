from fastapi import FastAPI, Depends

from routers.books import router as book_router
from routers.review import router as review_router
from routers.common import router as common_router
from routers.inventory import router as inventory_router
from routers.authors import router as author_router
from routers.orders import router as order_router

from services.background_service import cache_invalidator
import asyncio

from kafka_client.analitics_worker import AnaliticsWorker
app = FastAPI()

# Dependency

app.include_router(book_router)
app.include_router(review_router)
app.include_router(author_router)
app.include_router(common_router)
app.include_router(inventory_router)
app.include_router(order_router)

worker = AnaliticsWorker("book_views")
@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.on_event("startup")
async def startup_event():
    worker.start()
    asyncio.create_task(cache_invalidator())

@app.on_event("shutdown")
async def shutdown_event():
    worker.stop()