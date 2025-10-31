from fastapi import FastAPI, Depends

from routers.books import router as book_router
from routers.review import router as review_router


app = FastAPI()

# Dependency

app.include_router(book_router)
app.include_router(review_router)

@app.get("/")
async def root():
    return {"message": "Hello World"}