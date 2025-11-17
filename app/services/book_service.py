import asyncio
import inspect
import json
from threading import Thread
from concurrent.futures import ThreadPoolExecutor
from fastapi import BackgroundTasks
from kafka_client.producer import producer


# --- фоновая отправка в Kafka ---
def send_book_view_event(topic: str, book_id: int):
    payload = json.dumps({"book_id": book_id}).encode("utf-8")
    producer.produce(topic=topic, value=payload)
    producer.flush()


async def send_book_view_in_thread(topic: str, book_id: int):
    await asyncio.to_thread(send_book_view_event, topic, book_id)


# --- helpers ---

def _run_coro_in_new_thread(coro):
    """Выполнить корутину в отдельном потоке с новым event loop и вернуть результат."""
    def _runner():
        nonlocal result, error
        try:
            result = asyncio.run(coro)
        except BaseException as e:
            error = e

    result = None
    error = None
    t = Thread(target=_runner, daemon=True)
    t.start()
    t.join()
    if error:
        raise error
    return result


def _syncify(v):
    """
    Гарантировать НЕкорутиновый результат:
    - если v не awaitable -> вернуть как есть
    - если awaitable:
        * если нет запущенного loop -> asyncio.run(v)
        * если loop уже крутится -> выполнить в отдельном потоке
    """
    if not inspect.isawaitable(v):
        return v
    try:
        asyncio.get_running_loop()  # есть активный loop в текущем потоке
        return _run_coro_in_new_thread(v)
    except RuntimeError:
        # loop не запущен — можно просто выполнить тут
        return asyncio.run(v)


async def _await_maybe(v):
    return await v if inspect.isawaitable(v) else v


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
        # важно: этот метод ДОЛЖЕН возвращать готовый результат, не корутину
        if background_tasks:
            background_tasks.add_task(send_book_view_in_thread, "book_views", book_id)
        res = self.repo.get_by_id(book_id)
        return _syncify(res)

    def update_by_id(self, book_id, new_data):
        res = _syncify(self.repo.update_by_id(book_id, new_data))
        if res and self.redis:
            pub = getattr(self.redis, "publish", None)
            if pub:
                try:
                    out = pub("cache:invalidate", str(book_id))
                    if inspect.isawaitable(out):
                        _syncify(out)
                except Exception:
                    pass
        return res

    def delete_by_id(self, book_id):
        return _syncify(self.repo.delete_by_id(book_id))

    # ---------- АСИНХРОННЫЕ методы (для FastAPI) ----------
    async def create_async(self, data, *, session=None):
        return await _await_maybe(self.repo.create(data, session=session))

    async def create_book_with_author_async(self, book_data, author_data, *, session=None):
        return await _await_maybe(
            self.repo.create_book_with_author(book_data, author_data, session=session)
        )

    async def get_by_id_async(
        self,
        book_id: int,
        background_tasks: BackgroundTasks | None = None,
        *,
        session=None,
    ):
        if background_tasks:
            background_tasks.add_task(send_book_view_in_thread, "book_views", book_id)
        return await _await_maybe(self.repo.get_by_id(book_id, session=session))

    async def update_by_id_async(self, book_id, new_data, *, session=None):
        res = await _await_maybe(self.repo.update_by_id(book_id, new_data, session=session))
        if res and self.redis:
            pub = getattr(self.redis, "publish", None)
            if pub:
                try:
                    out = pub("cache:invalidate", str(book_id))
                    if inspect.isawaitable(out):
                        await out
                except Exception:
                    pass
        return res

    async def delete_by_id_async(self, book_id, *, session=None):
        return await _await_maybe(self.repo.delete_by_id(book_id, session=session))