# app/services/__init__.py
import asyncio
import inspect
from threading import Thread
from typing import Any, Optional
from fastapi import BackgroundTasks

from .book_service import BookService as _BookService, send_book_view_in_thread  # type: ignore


def _syncify(v: Any) -> Any:
    if not inspect.isawaitable(v):
        return v
    try:
        asyncio.get_running_loop()  # уже есть loop => выполняем в отдельном потоке
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
        # loop не запущен — можно напрямую
        return asyncio.run(v)


def _get_by_id_sync(
    self: _BookService,
    book_id: int,
    background_tasks: Optional[BackgroundTasks] = None,
):
    if background_tasks:
        background_tasks.add_task(send_book_view_in_thread, "book_views", book_id)
    # Критично: разворачиваем любой awaitable в синхронное значение
    return _syncify(self.repo.get_by_id(book_id))


# ПАТЧИМ класс, чтобы даже при старом book_service get_by_id был синхронным
_BookService.get_by_id = _get_by_id_sync  # type: ignore[attr-defined]

BookService = _BookService

__all__ = ["BookService"]