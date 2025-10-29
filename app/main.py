from fastapi import FastAPI, Depends

from routers.books import router


app = FastAPI()

# Dependency

app.include_router(router)

@app.get("/")
async def root():
    return {"message": "Hello World"}