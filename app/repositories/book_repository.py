import json
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.exc import OperationalError, ProgrammingError

from database.postgres_client import SessionLocal
# импортируем engine, чтобы уметь создать таблицы, когда их ещё нет
from database.postgres_client import engine  # type: ignore
from models import Book, Author, Base  # type: ignore
from schemas import AuthorSchema, BookSchema

# Redis может быть недоступен в тестах/CI — оборачиваем в try/except
try:
    from database.redis_client import redis_client as redis  # type: ignore
    from redis.exceptions import RedisError  # type: ignore
except Exception:  # pragma: no cover
    redis = None
    class RedisError(Exception): ...
    

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
        """
        Ленивая инициализация схемы БД.
        Безопасно вызывать много раз: create_all идемпотентен.
        """
        try:
            async with engine.begin() as conn:  # type: ignore[name-defined]
                await conn.run_sync(Base.metadata.create_all)  # type: ignore[name-defined]
        except Exception:
            # Не душним в рантайме: если не удалось — дадим нормальной ошибке ниже проявиться
            pass

    @staticmethod
    def _get(obj: Any, key: str, default=None):
        """Достаёт поле как из dict, так и из Pydantic/объектов."""
        if isinstance(obj, dict):
            return obj.get(key, default)
        return getattr(obj, key, default)

    # ---------- public API ----------

    async def get_by_id(self, book_id: int) -> BookSchema | None:
        cache_key = f"book:{book_id}"
        cached = await self._redis_get(cache_key)
        if cached:
            try:
                payload = json.loads(cached)
                return BookSchema(**payload)
            except Exception:
                # если кэш битый — просто игнорируем
                pass

        async with self.session_maker() as session:
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
                ) if book.author else None,
            )

        # вне транзакции — не блокируем БД, ошибки кэша игнорируем
        await self._redis_set(cache_key, json.dumps(book_schema.dict(), ensure_ascii=False), ex=self.ttl)
        return book_schema

    async def create(self, book_data: dict) -> BookSchema:
        # гарантируем, что таблицы есть (особенно актуально для sqlite в CI)
        await self._ensure_schema()

        async with self.session_maker() as session:
            try:
                book = Book(**book_data)
                session.add(book)
                await session.commit()
                await session.refresh(book)
            except (OperationalError, ProgrammingError):
                # если кто-то удалил таблицы — создадим и повторим один раз
                await self._ensure_schema()
                book = Book(**book_data)
                session.add(book)
                await session.commit()
                await session.refresh(book)

            schema = BookSchema(
                id=book.id,
                title=book.title,
                genre=book.genre,
                author=None,
            )

        # инвалидации тут не требуется — ключа ещё нет
        return schema

    async def update_by_id(self, book_id: int, new_data: dict) -> BookSchema | None:
        await self._ensure_schema()

        async with self.session_maker() as session:
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

                for key, value in new_data.items():
                    if hasattr(book, key):
                        setattr(book, key, value)

                # flush, чтобы получить актуальные поля до возврата
                await session.flush()

                book_schema = BookSchema(
                    id=book.id,
                    title=book.title,
                    genre=book.genre,
                    author=AuthorSchema(
                        id=book.author.id,
                        name=book.author.name,
                    ) if book.author else None,
                )

        await self._redis_delete(f"book:{book_id}")
        return book_schema

    async def delete_by_id(self, book_id: int) -> bool:
        await self._ensure_schema()

        async with self.session_maker() as session:
            stmt = select(Book).where(Book.id == book_id)
            result = await session.execute(stmt)
            book: Optional[Book] = result.scalars().first()
            if not book:
                return False

            await session.delete(book)
            await session.commit()

        await self._redis_delete(f"book:{book_id}")
        return True

    async def create_book_with_author(self, book_data: dict | Any, author_data: dict | Any) -> BookSchema:
        """
        Создаёт книгу и автора в одной транзакции.
        Если добавление автора упадёт, книга не сохраняется.
        """
        await self._ensure_schema()

        async with self.session_maker() as session:
            async with session.begin():
                author = Author(name=self._get(author_data, "name"))
                session.add(author)

                book = Book(
                    title=self._get(book_data, "title"),
                    genre=self._get(book_data, "genre"),
                    author=author,
                )
                session.add(book)

                # чтобы у объектов появились id до возврата
                await session.flush()

                return BookSchema(
                    id=book.id,
                    title=book.title,
                    genre=book.genre,
                    author=AuthorSchema(
                        id=author.id,
                        name=author.name,
                    ),
                )