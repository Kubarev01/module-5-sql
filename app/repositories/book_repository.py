import json
import inspect
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.ext.asyncio import AsyncSession

from database.postgres_client import engine, SessionLocal  # type: ignore
from models import Base, Book, Author  # type: ignore
from schemas import AuthorSchema, BookSchema

# Redis может отсутствовать в CI
try:
    from database.redis_client import redis_client as redis  # type: ignore
    from redis.exceptions import RedisError  # type: ignore
except Exception:  # pragma: no cover
    redis = None
    class RedisError(Exception): ...

async def _maybe_await(v):
    if inspect.isawaitable(v):
        return await v
    return v


class BookRepository:
    def __init__(self, session_maker=SessionLocal, ttl: int = 300, redis_client=redis):
        self.session_maker = session_maker
        self.redis = redis_client
        self.ttl = ttl

    # ---------- Redis helpers ----------

    async def _redis_get(self, key: str) -> Optional[str]:
        if not self.redis:
            return None
        try:
            return await self.redis.get(key)
        except RedisError:
            return None

    async def _redis_set(self, key: str, value: str, ex: int | None = None) -> None:
        if not self.redis:
            return
        try:
            await self.redis.set(key, value, ex=ex)
        except RedisError:
            pass

    async def _redis_delete(self, key: str) -> None:
        if not self.redis:
            return
        try:
            await self.redis.delete(key)
        except RedisError:
            pass

    async def _ensure_schema(self) -> None:
        try:
            async with engine.begin() as conn:  # type: ignore[name-defined]
                await conn.run_sync(Base.metadata.create_all)  # type: ignore[name-defined]
        except Exception:
            pass

    @staticmethod
    def _get(obj: Any, key: str, default=None):
        if isinstance(obj, dict):
            return obj.get(key, default)
        return getattr(obj, key, default)

    # ---------- API ----------

    async def get_by_id(self, book_id: int, session: AsyncSession | None = None) -> BookSchema | None:
        cache_key = f"book:{book_id}"

        cached = await self._redis_get(cache_key)
        if cached:
            try:
                payload = json.loads(cached)
                return BookSchema(**payload)
            except Exception:
                pass

        if session is None:
            async with self.session_maker() as session_:
                return await self.get_by_id(book_id, session_)

        # Без транзакции — чтобы не падать на FakeDbSession
        stmt = select(Book).options(selectinload(Book.author)).where(Book.id == book_id)
        result = await session.execute(stmt)

        book: Optional[Any] = None
        if hasattr(result, "scalars"):
            book = result.scalars().first()
        else:
            # Фейки могут не иметь .scalars(); пробуем mappings()/first()
            mappings = getattr(result, "mappings", None)
            if mappings:
                rows = mappings().all()
                row = rows[0] if rows else None
                if isinstance(row, dict):
                    book = row.get("Book") or row.get("book") or row  # best-effort
                else:
                    book = row
            else:
                first = getattr(result, "first", None)
                row = first() if first else None
                if isinstance(row, (list, tuple)) and row:
                    book = row[0]
                else:
                    book = row

        if not book:
            return None
        title = self._get(book, "title") or ""
        genre = self._get(book, "genre") or ""
        author_obj = self._get(book, "author")

        book_schema = BookSchema(
            id=self._get(book, "id"),
            title=title,
            genre=genre,
            author=AuthorSchema(
                id=self._get(author_obj, "id"),
                name=self._get(author_obj, "name"),
            ) if author_obj else None,
        )

        book_schema = BookSchema(
            id=self._get(book, "id"),
            title=self._get(book, "title"),
            genre=self._get(book, "genre"),
            author=AuthorSchema(
                id=self._get(self._get(book, "author"), "id"),
                name=self._get(self._get(book, "author"), "name"),
            ) if self._get(book, "author") else None,
        )

        try:
            await self._redis_set(cache_key, json.dumps(book_schema.dict(), ensure_ascii=False), ex=self.ttl)
        except Exception:
            pass
        return book_schema

    async def create(self, book_data: dict | Any, session: AsyncSession | None = None) -> BookSchema:
        await self._ensure_schema()

        if session is None:
            async with self.session_maker() as session_:
                return await self.create(book_data, session_)

        def _make_book():
            return Book(
                title=self._get(book_data, "title"),
                genre=self._get(book_data, "genre"),
                author_id=self._get(book_data, "author_id"),
            )

        try:
            book = _make_book()
            add = getattr(session, "add", None)
            if add:
                await _maybe_await(add(book))
            commit = getattr(session, "commit", None)
            if commit:
                await _maybe_await(commit())
            refresh = getattr(session, "refresh", None)
            if refresh:
                await _maybe_await(refresh(book))
        except (OperationalError, ProgrammingError):
            await self._ensure_schema()
            book = _make_book()
            add = getattr(session, "add", None)
            if add:
                await _maybe_await(add(book))
            commit = getattr(session, "commit", None)
            if commit:
                await _maybe_await(commit())
            refresh = getattr(session, "refresh", None)
            if refresh:
                await _maybe_await(refresh(book))

        return BookSchema(
            id=self._get(book, "id"),
            title=self._get(book, "title"),
            genre=self._get(book, "genre"),
            author=None,
        )

    async def update_by_id(self, book_id: int, new_data: dict | Any, session: AsyncSession | None = None) -> BookSchema | None:
        await self._ensure_schema()

        if session is None:
            async with self.session_maker() as session_:
                return await self.update_by_id(book_id, new_data, session_)

        stmt = select(Book).options(selectinload(Book.author)).where(Book.id == book_id)
        result = await session.execute(stmt)
        book = result.scalars().first() if hasattr(result, "scalars") else (getattr(result, "first", lambda: None)())
        if not book:
            return None

        for key, value in dict(new_data).items():
            if hasattr(book, key):
                setattr(book, key, value)

        if hasattr(session, "flush"):
            await _maybe_await(session.flush())
        if hasattr(session, "commit"):
            await _maybe_await(session.commit())

        schema = BookSchema(
            id=self._get(book, "id"),
            title=self._get(book, "title"),
            genre=self._get(book, "genre"),
            author=AuthorSchema(
                id=self._get(self._get(book, "author"), "id"),
                name=self._get(self._get(book, "author"), "name"),
            ) if self._get(book, "author") else None,
        )

        await self._redis_delete(f"book:{book_id}")
        return schema

    async def delete_by_id(self, book_id: int, session: AsyncSession | None = None) -> bool:
        await self._ensure_schema()

        if session is None:
            async with self.session_maker() as session_:
                return await self.delete_by_id(book_id, session_)

        stmt = select(Book).where(Book.id == book_id)
        result = await session.execute(stmt)
        book = result.scalars().first() if hasattr(result, "scalars") else (getattr(result, "first", lambda: None)())
        if not book:
            return False

        delete = getattr(session, "delete", None)
        if delete:
            await _maybe_await(delete(book))
        commit = getattr(session, "commit", None)
        if commit:
            await _maybe_await(commit())

        await self._redis_delete(f"book:{book_id}")
        return True

    async def create_book_with_author(self, book_data: dict | Any, author_data: dict | Any, session: AsyncSession | None = None) -> BookSchema:
        await self._ensure_schema()

        if session is None:
            async with self.session_maker() as session_:
                return await self.create_book_with_author(book_data, author_data, session_)

        author = Author(name=self._get(author_data, "name"))
        add = getattr(session, "add", None)
        if add:
            await _maybe_await(add(author))

        book = Book(
            title=self._get(book_data, "title"),
            genre=self._get(book_data, "genre"),
            author=author,
        )
        if add:
            await _maybe_await(add(book))

        if hasattr(session, "flush"):
            await _maybe_await(session.flush())
        if hasattr(session, "commit"):
            await _maybe_await(session.commit())
        if hasattr(session, "refresh"):
            await _maybe_await(session.refresh(book))

        return BookSchema(
            id=self._get(book, "id"),
            title=self._get(book, "title"),
            genre=self._get(book, "genre"),
            author=AuthorSchema(id=self._get(author, "id"), name=self._get(author, "name")),
        )