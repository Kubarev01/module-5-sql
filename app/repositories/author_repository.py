import json
from typing import Iterable

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from database.postgres_client import SessionLocal
from database.redis_client import redis_client as redis
from models import Author, Book
from schemas import AuthorSchema, BookSchema


class AuthorRepository:
    def __init__(self, session_maker=SessionLocal, ttl: int = 300):
        self.session_maker = session_maker
        self.redis = redis
        self.ttl = ttl
    def _as_book_schema_list(self, books: Iterable[Book]) -> list[BookSchema]:
        return [
            BookSchema(
                id=book.id,
                title=book.title,
                genre=book.genre,
                author_id=book.author_id
            )
            for book in books
        ]
   
    async def get_by_id(self, author_id: int) -> AuthorSchema | None:
        cached = await self.redis.get(f"author:{author_id}")
        if cached:
            payload = json.loads(cached)
            return AuthorSchema(**payload)

        async with self.session_maker() as session:
            stmt = (
                select(Author)
                .options(selectinload(Author.books))
                .where(Author.id == author_id)
            )
            result = await session.execute(stmt)
            author: Author | None = result.scalars().first()

            if not author:
                return None

            author_schema = AuthorSchema(
                id=author.id,
                name=author.name,
                books=self._as_book_schema_list(author.books)
            )
            await self.redis.set(
                f"author:{author_id}",
                json.dumps(author_schema.dict()),
                ex=self.ttl
            )
            return author_schema

 
    async def create(self, author_data: dict) -> AuthorSchema:
        async with self.session_maker() as session:
            author = Author(**author_data)
            session.add(author)
            await session.commit()
            await session.refresh(author)

            return AuthorSchema(
                id=author.id,
                name=author.name,
                books=[]
            )

    
    async def update_by_id(self, author_id: int, new_data: dict) -> AuthorSchema | None:
        async with self.session_maker() as session:
            async with session.begin():
                stmt = (
                    select(Author)
                    .options(selectinload(Author.books))
                    .where(Author.id == author_id)
                )
                result = await session.execute(stmt)
                author: Author | None = result.scalars().first()
                if not author:
                    return None

                for key, value in new_data.items():
                    if hasattr(author, key):
                        setattr(author, key, value)

                await session.flush()

                author_schema = AuthorSchema(
                    id=author.id,
                    name=author.name,
                    books=self._as_book_schema_list(author.books)
                )

     
        await self.redis.delete(f"author:{author_id}")

        for b in author_schema.books:
            await self.redis.delete(f"book:{b.id}")

        return author_schema


    async def delete_by_id(self, author_id: int) -> bool:
        async with self.session_maker() as session:
            stmt = (
                select(Author)
                .options(selectinload(Author.books))
                .where(Author.id == author_id)
            )
            result = await session.execute(stmt)
            author: Author | None = result.scalars().first()

            if not author:
                return False

         
            book_ids = [b.id for b in (author.books or [])]

            await session.delete(author)
            await session.commit()

        await self.redis.delete(f"author:{author_id}")
        for book_id in book_ids:
            await self.redis.delete(f"book:{book_id}")

        return True


    async def create_author_with_books(self, author_data, books_data: list[dict]) -> AuthorSchema:
        """
        Создает автора и список его книг в одной транзакции.
        Если создание любой из книг упадет, автор не будет сохранен.
        `author_data` и элементы `books_data` могут быть dict или pydantic-моделями c .dict()
        """
        def as_dict(obj):
            if hasattr(obj, "dict"):
                return obj.dict()
            return obj

        async with self.session_maker() as session:
            async with session.begin():
                author = Author(**as_dict(author_data))
                session.add(author)

                books = []
                for b in books_data or []:
                    b_dict = as_dict(b)
                    book = Book(
                        title=b_dict.get("title"),
                        genre=b_dict.get("genre"),
                        author=author,
                    )
                    session.add(book)
                    books.append(book)

                await session.flush()

                return AuthorSchema(
                    id=author.id,
                    name=author.name,
                    books=self._as_book_schema_list(books)
                )