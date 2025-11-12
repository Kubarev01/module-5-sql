import asyncio
import redis.asyncio as redis
import os

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")

redis_client = redis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)

async def test_connection():
    try:
        response = await redis_client.ping()
        print("✅ Redis подключен:", response)
    except Exception as e:
        print("❌ Ошибка подключения к Redis:", e)

if __name__ == "__main__":
    asyncio.run(test_connection())