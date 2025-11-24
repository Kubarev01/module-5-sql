from app.logging_config import logging
from app.database.redis_client import redis_client as redis  

async def cache_invalidator():
    logging.info("🔔 Cache invalidator started")

    pubsub = redis.pubsub()
    await pubsub.subscribe("cache:invalidate")
    logging.info("✅ Подписан на канал cache:invalidate")

    async for message in pubsub.listen():
        if message["type"] != "message":
            continue

        book_id = message["data"]
        if isinstance(book_id, bytes):
            book_id = book_id.decode()

        deleted = await redis.delete(f"book:{book_id}")
        logging.info(f"🗑 Кэш для book:{book_id} сброшен, удалено ключей: {deleted}")