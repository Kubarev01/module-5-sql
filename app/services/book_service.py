import asyncio
import inspect
import json
from threading import Thread
from fastapi import BackgroundTasks
from kafka_client.producer import producer

# --- фоновая отправка в Kafka ---
def send_book_view_event(topic: str, book_id: int):
    payload = json.dumps({"book_id": book_id}).encode("utf-8")
    producer.produce(topic=topic, value=payload)
    producer.flush()

async def send_book_view_in_thread(topic: str, book_id: int):
    await asyncio.to_thread(send_book_view_event, topic, book_id)

# --- хелперы ---

def _syncify(v):
    """Если v awaitable — исполним его и вернём результат.
    Работает и когда текущий поток уже с event-loop (otel и т.п.)."""
    if not inspect.isawaitable(v):
        return v

    try:
        asyncio.get_running_loop()  # есть запущенный loop в этом потоке
        box, err = {}, {}

        def runner():
            try:
                box["v"] = asyncio.run(v)
            except BaseException as e:
                err["e"] = e

        t = Thread(target=runner, daemon=True)
        t.start()
        t.join()
        if "e" in err:
            raise err["e"]
        return box.get("v")
    except RuntimeError:
        # loop не запущен — можно просто asyncio.run
        return asyncio.run(v)
    
async def _await_maybe(v):
    return await v if inspect.isawaitable(v) else v


class BookService:
    def __init__(self, repo, redis=None):
        self.repo = repo
        self.redis = redis

    # ---------- СИНХРОННЫЕ методы для unit-тестов ----------
    def create(self, data):
        return _syncify(self.repo.create(data))

    def create_book_with_author(self, book_data, author_data):
        return _syncify(self.repo.create_book_with_author(book_data, author_data))

    def get_by_id(self, book_id: int, background_tasks: BackgroundTasks | None = None):
        if background_tasks:
            background_tasks.add_task(send_book_view_in_thread, "book_views", book_id)
        return _syncify(self.repo.get_by_id(book_id))

    def update_by_id(self, book_id, new_data):
        result = _syncify(self.repo.update_by_id(book_id, new_data))
        # если получилось синхронное значение — оповестим редис (если есть)
        if not inspect.isawaitable(result) and result and self.redis:
            pub = getattr(self.redis, "publish", None)
            if pub:
                try:
                    out = pub("cache:invalidate", str(book_id))
                    if inspect.isawaitable(out):
                        asyncio.run(out)
                except Exception:
                    pass
        return result

    def delete_by_id(self, book_id):
        return _syncify(self.repo.delete_by_id(book_id))

    # ---------- АСИНХРОННЫЕ методы для FastAPI ----------
    async def create_async(self, data, *, session=None):
        return await _await_maybe(self.repo.create(data, session=session))

    async def create_book_with_author_async(self, book_data, author_data, *, session=None):
        return await _await_maybe(
            self.repo.create_book_with_author(book_data, author_data, session=session)
        )

    async def get_by_id_async(self, book_id: int, background_tasks: BackgroundTasks | None = None, *, session=None):
        if background_tasks:
            background_tasks.add_task(send_book_view_in_thread, "book_views", book_id)
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