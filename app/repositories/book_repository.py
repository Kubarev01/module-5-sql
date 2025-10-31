import json
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from database.postgres_client import SessionLocal
from models import Book, Author
from schemas import AuthorSchema, BookSchema
from database.redis_client import redis_client as redis

class BookRepository:
    def __init__(self, session_maker=SessionLocal, ttl: int = 300):
        self.session_maker = session_maker
        self.redis = redis
        self.ttl = ttl

    async def get_by_id(self, book_id: int):
        cached_book = await self.redis.get(f"book:{book_id}")
        if cached_book:
            payload = json.loads(cached_book)
            return BookSchema(**payload)

        async with self.session_maker() as session:
            stmt = select(Book).options(selectinload(Book.author)).where(Book.id == book_id)
            result = await session.execute(stmt)
            book = result.scalars().first()
            if book:
                book_schema = BookSchema(
                    id=book.id,
                    title=book.title,
                    genre=book.genre,
                    author=AuthorSchema(
                        id=book.author.id,
                        name=book.author.name
                    ) if book.author else None
                )
                await self.redis.set(f"book:{book_id}", json.dumps(book_schema.dict()), ex=self.ttl)
                return book_schema
        return None

    async def create(self, book_data: dict):
        async with self.session_maker() as session:
            book = Book(**book_data)
            session.add(book)
            await session.commit()
            await session.refresh(book)
            return BookSchema(
                id=book.id,
                title=book.title,
                genre=book.genre,
                author=None  
            )

    async def update_by_id(self, book_id: int, new_data: dict) -> BookSchema | None:
        async with self.session_maker() as session:
            async with session.begin():
                stmt = select(Book).options(selectinload(Book.author)).where(Book.id == book_id)
                result = await session.execute(stmt)
                book = result.scalars().first()
                if not book:
                    return None

                for key, value in new_data.items():
                    if hasattr(book, key):
                        setattr(book, key, value)

                await session.flush()

                book_schema = BookSchema(
                    id=book.id,
                    title=book.title,
                    genre=book.genre,
                    author=AuthorSchema(
                        id=book.author.id,
                        name=book.author.name
                    ) if book.author else None
                )

        await self.redis.delete(f"book:{book_id}")
        return book_schema

    async def delete_by_id(self, book_id: int) -> bool:
        async with self.session_maker() as session:
            stmt = select(Book).where(Book.id == book_id)
            result = await session.execute(stmt)
            book = result.scalars().first()
            if book:
                await session.delete(book)
                await session.commit()
                await self.redis.delete(f"book:{book_id}")
                return True
            return False

    async def create_book_with_author(self, book_data: dict, author_data: dict) -> BookSchema:
        """
        Создаёт книгу и автора в одной транзакции.
        Если добавление автора упадёт, книга не сохраняется.
        """
        async with self.session_maker() as session:
            async with session.begin():
                author = Author(name=author_data.name)
                session.add(author)

                book = Book(
                    title=book_data.title,
                    genre=book_data.genre,
                    author=author
                )
                session.add(book)

                await session.flush() 

                return BookSchema(
                    id=book.id,
                    title=book.title,
                    genre=book.genre,
                    author=AuthorSchema(
                        id=author.id,
                        name=author.name
                    )
                )
            