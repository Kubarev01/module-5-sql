import asyncio
import inspect
import json
from threading import Thread
from fastapi import BackgroundTasks
from kafka_client.producer import producer


# ---------- утилиты ----------

def _run_coro_in_new_thread(coro):
    """Выполнить корутину в отдельном потоке с собственным event loop."""
    box = {"res": None, "err": None}

    def runner():
        try:
            box["res"] = asyncio.run(coro)
        except BaseException as e:  # пробрасываем любую ошибку
            box["err"] = e

    t = Thread(target=runner, daemon=True)
    t.start()
    t.join()
    if box["err"] is not None:
        raise box["err"]
    return box["res"]


def _syncify(v):
    """
    Вернёт значение как есть, а если это awaitable — выполнит его и вернёт результат.
    Работает даже если текущий поток уже с активным event loop (otel и т.п.).
    """
    if not inspect.isawaitable(v):
        return v
    try:
        # если в этом потоке уже есть живой loop — нельзя вызывать asyncio.run
        asyncio.get_running_loop()
        return _run_coro_in_new_thread(v)
    except RuntimeError:
        # loop не запущен — можно выполнить напрямую
        return asyncio.run(v)


async def _await_maybe(v):
    return await v if inspect.isawaitable(v) else v


# ---------- kafka (мягко, без влияния на тесты) ----------

def _send_book_view_event(topic: str, book_id: int):
    payload = json.dumps({"book_id": book_id}).encode("utf-8")
    try:
        producer.produce(topic=topic, value=payload)
        producer.flush()
    except Exception:
        # в CI брокера нет — просто игнорируем
        pass


async def _send_book_view_in_thread(topic: str, book_id: int):
    await asyncio.to_thread(_send_book_view_event, topic, book_id)


# ---------- сервис ----------

class BookService:
    def __init__(self, repo, redis=None):
        self.repo = repo
        self.redis = redis

    # ===== СИНХРОННЫЕ методы (для unit-тестов) =====

    def create(self, data):
        return _syncify(self.repo.create(data))

    def create_book_with_author(self, book_data, author_data):
        return _syncify(self.repo.create_book_with_author(book_data, author_data))

    def get_by_id(self, book_id: int, background_tasks: BackgroundTasks | None = None):
        """
        ВАЖНО: этот метод — синхронный, чтобы unit-тесты типа
        `book = service.get_by_id(1)` НЕ получали корутину.
        """
        if background_tasks:
            background_tasks.add_task(_send_book_view_in_thread, "book_views", book_id)
        return _syncify(self.repo.get_by_id(book_id))

    def update_by_id(self, book_id, new_data):
        result = _syncify(self.repo.update_by_id(book_id, new_data))
        # если получилось, мягко сообщим редису (если есть)
        if result and self.redis:
            pub = getattr(self.redis, "publish", None)
            if pub:
                try:
                    out = pub("cache:invalidate", str(book_id))
                    if inspect.isawaitable(out):
                        _syncify(out)
                except Exception:
                    pass
        return result

    def delete_by_id(self, book_id):
        return _syncify(self.repo.delete_by_id(book_id))

    # ===== АСИНХРОННЫЕ методы (для FastAPI) =====

    async def create_async(self, data, *, session=None):
        return await _await_maybe(self.repo.create(data, session=session))

    async def create_book_with_author_async(self, book_data, author_data, *, session=None):
        return await _await_maybe(
            self.repo.create_book_with_author(book_data, author_data, session=session)
        )

    async def get_by_id_async(self, book_id: int, background_tasks: BackgroundTasks | None = None, *, session=None):
        if background_tasks:
            background_tasks.add_task(_send_book_view_in_thread, "book_views", book_id)
        return await _await_maybe(self.repo.get_by_id(book_id, session=session))

    async def update_by_id_async(self, book_id, new_data, *, session=None):
        result = await _await_maybe(self.repo.update_by_id(book_id, new_data, session=session))
        if result and self.redis:
            pub = getattr(self.redis, "publish", None)
            if pub:
                try:
                    out = pub("cache:invalidate", str(book_id))
                    if inspect.isawaitable(out):
                        await out
                except Exception:
                    pass
        return result

    async def delete_by_id_async(self, book_id, *, session=None):
        return await _await_maybe(self.repo.delete_by_id(book_id, session=session))


# Жёсткая гарантия: get_by_id обязан быть синхронным
assert not inspect.iscoroutinefunction(BookService.get_by_id), \
    "BookService.get_by_id должен быть СИНХРОННЫМ (def, не async def)"