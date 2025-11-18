# app/services/__init__.py
"""
Lightweight package init to avoid import-time crashes in tests/CI.
No eager re-exports; use lazy access for optional conveniences.
"""

from importlib import import_module

__all__ = ["BookService", "AuthorService", "send_book_view_in_thread"]

def __getattr__(name):
    if name == "BookService":
        return import_module(".book_service", __name__).BookService
    if name == "AuthorService":
        return import_module(".author_service", __name__).AuthorService
    if name == "send_book_view_in_thread":
        # 1) если функция есть в book_service — используем её,
        # 2) иначе пробуем background_service,
        # 3) иначе возвращаем no-op корутину.
        try:
            mod = import_module(".book_service", __name__)
            fn = getattr(mod, "send_book_view_in_thread", None)
            if fn:
                return fn
        except Exception:
            pass
        try:
            mod = import_module(".background_service", __name__)
            fn = getattr(mod, "send_book_view_in_thread", None)
            if fn:
                return fn
        except Exception:
            pass

        async def _noop(*args, **kwargs):
            return None
        return _noop

    raise AttributeError(name)