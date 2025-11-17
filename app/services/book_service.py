# app/services/book_service.py
import asyncio
import inspect
import json
from threading import Thread
from fastapi import BackgroundTasks
from kafka_client.producer import producer


def _run_coro_in_new_thread(coro):
    box = {"res": None, "err": None}

    def runner():
        try:
            box["res"] = asyncio.run(coro)
        except BaseException as e:
            box["err"] = e

    t = Thread(target=runner, daemon=True)
    t.start()
    t.join()
    if box["err"] is not None:
        raise box["err"]
    return box["res"]


def _syncify(v):
    """Вернёт значение сразу; если это awaitable — выполнит и вернёт результат."""
    if not inspect.isawaitable(v):
        return v
    # если уже есть рабочий event loop в этом потоке — уходим в новый поток
    try:
        asyncio.get_running_loop()
        return _run_coro_in_new_thread(v)
    except RuntimeError:
        # loop не запущен — можно выполнить напрямую
        return asyncio.run(v)


# --- Kafka (мягко, не мешает тестам) ---
def _send_book_view_event(topic: str, book_id: int):
    payload = json.dumps({"book_id": book_id}).encode("utf-8")
    producer.produce(topic=topic, value=payload)
    producer.flush()


async def _send_book_view_in_thread(topic: str, book_id: int):
    await asyncio.to_thread(_send_book_view_event, topic, book_id)


class BookService:
    def __init__(self, repo, redis=None):
        self.repo = repo
        self.redis = redis

    # ---------- СИНХРОННЫЕ методы (для unit-тестов) ----------
    def create(self, data):
        return _syncify(self.repo.create(data))

    def create_book_with_author(self, book_data, author_data):
        return _syncify(self.repo.create_book_with_author(book_data, author_data))

    def get_by_id(self, book_id: int, background_tasks: BackgroundTasks | None = None):
        # ВАЖНО: этот метод — СИНХРОННЫЙ. Он возвращает dict/схему, а не корутину.
        if background_tasks:
            background_tasks.add_task(_send_book_view_in_thread, "book_views", book_id)
        return _syncify(self.repo.get_by_id(book_id))

    def update_by_id(self, book_id, new_data):
        res = _syncify(self.repo.update_by_id(book_id, new_data))
        if res and self.redis:
            pub = getattr(self.redis, "publish", None)
            if pub:
                out = pub("cache:invalidate", str(book_id))
                if inspect.isawaitable(out):
                    _syncify(out)
        return res

    def delete_by_id(self, book_id):
        return _syncify(self.repo.delete_by_id(book_id))

    # ---------- АСИНХРОННЫЕ методы (для FastAPI) ----------
    async def create_async(self, data, *, session=None):
        v = self.repo.create(data, session=session)
        return await v if inspect.isawaitable(v) else v

    async def create_book_with_author_async(self, book_data, author_data, *, session=None):
        v = self.repo.create_book_with_author(book_data, author_data, session=session)
        return await v if inspect.isawaitable(v) else v

    async def get_by_id_async(self, book_id: int, background_tasks: BackgroundTasks | None = None, *, session=None):
        if background_tasks:
            background_tasks.add_task(_send_book_view_in_thread, "book_views", book_id)
        v = self.repo.get_by_id(book_id, session=session)
        return await v if inspect.isawaitable(v) else v

    async def update_by_id_async(self, book_id, new_data, *, session=None):
        v = self.repo.update_by_id(book_id, new_data, session=session)
        res = await v if inspect.isawaitable(v) else v
        if res and self.redis:
            pub = getattr(self.redis, "publish", None)
            if pub:
                out = pub("cache:invalidate", str(book_id))
                if inspect.isawaitable(out):
                    await out
        return res

    async def delete_by_id_async(self, book_id, *, session=None):
        v = self.repo.delete_by_id(book_id, session=session)
        return await v if inspect.isawaitable(v) else v


# Жёсткая проверка на этапе импорта: get_by_id обязан быть синхронным.
assert not inspect.iscoroutinefunction(BookService.get_by_id), "BookService.get_by_id должен быть синхронным"