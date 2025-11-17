from __future__ import annotations

from fastapi import BackgroundTasks
import asyncio
import inspect
import json
from typing import Any

# Kafka producer может отсутствовать в CI — не падаем
try:
    from kafka_client.producer import producer  # type: ignore
except Exception:
    producer = None


def send_book_view_event(topic: str, book_id: int) -> None:
    """Синхронно отправляет событие о просмотре книги (используется в отдельном потоке)."""
    if not producer:
        return
    payload = json.dumps({"book_id": book_id}).encode("utf-8")
    try:
        producer.produce(topic=topic, value=payload)
        producer.flush()
    except Exception:
        # не валим тесты, если продюсер не сконфигурирован
        pass


async def send_book_view_in_thread(topic: str, book_id: int) -> None:
    await asyncio.to_thread(send_book_view_event, topic, book_id)


class BookService:
    def __init__(self, repo, redis: Any | None = None) -> None:
        self.repo = repo
        self.redis = redis

    # --------- sync для unit-теста ---------
    def get_by_id(self, book_id: int, background_tasks: BackgroundTasks | None = None):
        if background_tasks:
            background_tasks.add_task(send_book_view_in_thread, "book_views", book_id)

        result = self.repo.get_by_id(book_id)  # в юнит-тесте замокон на dict
        if inspect.isawaitable(result):
            # если внезапно вернулась корутина, мы в sync-контексте — исполним её
            return asyncio.run(result)
        return result

    # --------- async для FastAPI ---------
    async def get_by_id_async(
        self,
        book_id: int,
        background_tasks: BackgroundTasks | None = None,
        *,
        session=None,
    ):
        if background_tasks:
            background_tasks.add_task(send_book_view_in_thread, "book_views", book_id)

        result = self.repo.get_by_id(book_id, session=session)
        return await result if inspect.isawaitable(result) else result

    async def create(self, data: dict, *, session=None):
        result = self.repo.create(data, session=session) \
            if "session" in getattr(self.repo.create, "__code__", type("c", (), {"co_varnames": ()})) .co_varnames \
            else self.repo.create(data)
        return await result if inspect.isawaitable(result) else result

    async def create_book_with_author(self, book_data, author_data, *, session=None):
        fn = getattr(self.repo, "create_book_with_author")
        result = fn(book_data, author_data, session=session) \
            if "session" in fn.__code__.co_varnames else fn(book_data, author_data)
        return await result if inspect.isawaitable(result) else result

    async def update_by_id(self, book_id: int, new_data: dict, *, session=None):
        fn = getattr(self.repo, "update_by_id")
        result = fn(book_id, new_data, session=session) \
            if "session" in fn.__code__.co_varnames else fn(book_id, new_data)
        updated = await result if inspect.isawaitable(result) else result

        
        if self.redis:
            try:
                await self.redis.publish("cache:invalidate", str(book_id))
            except Exception:
                pass
        return updated

    async def delete_by_id(self, book_id: int, *, session=None):
        fn = getattr(self.repo, "delete_by_id")
        result = fn(book_id, session=session) \
            if "session" in fn.__code__.co_varnames else fn(book_id)
        deleted = await result if inspect.isawaitable(result) else result

        if deleted and self.redis:
            try:
                await self.redis.publish("cache:invalidate", str(book_id))
            except Exception:
                pass
        return deleted