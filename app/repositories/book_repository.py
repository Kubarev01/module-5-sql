import json
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.ext.asyncio import AsyncSession

# БД и модели
from database.postgres_client import engine, SessionLocal  # type: ignore
from models import Base, Book, Author  # type: ignore
from schemas import AuthorSchema, BookSchema

# Redis может быть недоступен (CI) — делаем мягкий импорт
try:
    from database.redis_client import redis_client as redis  # type: ignore
    from redis.exceptions import RedisError  # type: ignore
except Exception:  # pragma: no cover
    redis = None

    class RedisError(Exception):
        ...
# ------------------------------


class BookRepository:
    def __init__(self, session_maker=SessionLocal, ttl: int = 300, redis_client=redis):
        self.session_maker = session_maker
        self.redis = redis_client
        self.ttl = ttl

    # ---------- helpers ----------

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
        """Ленивая инициализация схемы БД (идемпотентно)."""
        try:
            async with engine.begin() as conn:  # type: ignore[name-defined]
                await conn.run_sync(Base.metadata.create_all)  # type: ignore[name-defined]
        except Exception:
            # В случае гонок/ограничений окружения дадим проявиться реальной ошибке ниже
            pass

    @staticmethod
    def _get(obj: Any, key: str, default=None):
        """Достаёт поле как из dict, так и из Pydantic/объектов."""
        if isinstance(obj, dict):
            return obj.get(key, default)
        return getattr(obj, key, default)

    # ---------- public API ----------

    async def get_by_id(
        self, book_id: int, session: AsyncSession | None = None
    ) -> BookSchema | None:
        cache_key = f"book:{book_id}"

        cached = await self._redis_get(cache_key)
        if cached:
            try:
                payload = json.loads(cached)
                return BookSchema(**payload)
            except Exception:
                # битый кэш — игнорируем
                pass

        if session is None:
            async with self.session_maker() as session_:
                return await self.get_by_id(book_id, session_)

        async with session.begin():
            stmt = (
                select(Book)
                .options(selectinload(Book.author))
                .where(Book.id == book_id)
            )
            result = await session.execute(stmt)
            book: Optional[Book] = result.scalars().first()
            if not book:
                return None

            book_schema = BookSchema(
                id=book.id,
                title=book.title,
                genre=book.genre,
                author=AuthorSchema(
                    id=book.author.id,
                    name=book.author.name,
                )
                if book.author
                else None,
            )

        # вне транзакции — не блокируем БД, ошибки кэша игнорируем
        try:
            await self._redis_set(
                cache_key,
                json.dumps(book_schema.dict(), ensure_ascii=False),
                ex=self.ttl,
            )
        except Exception:
            pass
        return book_schema

    async def create(
        self, book_data: dict | Any, session: AsyncSession | None = None
    ) -> BookSchema:
        await self._ensure_schema()

        if session is None:
            async with self.session_maker() as session_:
                return await self.create(book_data, session_)

        try:
            async with session.begin():
                book = Book(
                    title=self._get(book_data, "title"),
                    genre=self._get(book_data, "genre"),
                    author_id=self._get(book_data, "author_id"),
                )
                session.add(book)
            await session.refresh(book)
        except (OperationalError, ProgrammingError):
            # если таблиц нет — создадим и повторим один раз
            await self._ensure_schema()
            async with session.begin():
                book = Book(
                    title=self._get(book_data, "title"),
                    genre=self._get(book_data, "genre"),
                    author_id=self._get(book_data, "author_id"),
                )
                session.add(book)
            await session.refresh(book)

        return BookSchema(
            id=book.id,
            title=book.title,
            genre=book.genre,
            author=None,
        )

    async def update_by_id(
        self, book_id: int, new_data: dict | Any, session: AsyncSession | None = None
    ) -> BookSchema | None:
        await self._ensure_schema()

        if session is None:
            async with self.session_maker() as session_:
                return await self.update_by_id(book_id, new_data, session_)

        async with session.begin():
            stmt = (
                select(Book)
                .options(selectinload(Book.author))
                .where(Book.id == book_id)
            )
            result = await session.execute(stmt)
            book: Optional[Book] = result.scalars().first()
            if not book:
                return None

            for key, value in dict(new_data).items():
                if hasattr(book, key):
                    setattr(book, key, value)

            await session.flush()

            schema = BookSchema(
                id=book.id,
                title=book.title,
                genre=book.genre,
                author=AuthorSchema(
                    id=book.author.id,
                    name=book.author.name,
                )
                if book.author
                else None,
            )

        await self._redis_delete(f"book:{book_id}")
        return schema

    async def delete_by_id(
        self, book_id: int, session: AsyncSession | None = None
    ) -> bool:
        await self._ensure_schema()

        if session is None:
            async with self.session_maker() as session_:
                return await self.delete_by_id(book_id, session_)

        async with session.begin():
            stmt = select(Book).where(Book.id == book_id)
            result = await session.execute(stmt)
            book: Optional[Book] = result.scalars().first()
            if not book:
                return False

            await session.delete(book)

        await self._redis_delete(f"book:{book_id}")
        return True

    async def create_book_with_author(
        self,
        book_data: dict | Any,
        author_data: dict | Any,
        session: AsyncSession | None = None,
    ) -> BookSchema:
        """
        Создаёт книгу и автора в одной транзакции.
        Если добавление автора упадёт, книга не сохраняется.
        """
        await self._ensure_schema()

        if session is None:
            async with self.session_maker() as session_:
                return await self.create_book_with_author(book_data, author_data, session_)

        async with session.begin():
            author = Author(name=self._get(author_data, "name"))
            session.add(author)

            book = Book(
                title=self._get(book_data, "title"),
                genre=self._get(book_data, "genre"),
                author=author,
            )
            session.add(book)

            await session.flush()

            schema = BookSchema(
                id=book.id,
                title=book.title,
                genre=book.genre,
                author=AuthorSchema(id=author.id, name=author.name),
            )

        return schema