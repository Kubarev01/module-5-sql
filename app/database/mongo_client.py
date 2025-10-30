import motor.motor_asyncio
import os

MONGO_URL = os.getenv("MONGO_URL", "mongodb://mongodb:27017")

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