from sqlalchemy import select
from sqlalchemy.orm import selectinload
from database import SessionLocal
from models import Book, Author
from schemas import AuthorSchema, BookSchema
from fastapi import HTTPException

class BookRepository:
    def __init__(self, session_maker=SessionLocal):
        self.session_maker = session_maker

    async def get_by_id(self, book_id: int):
        async with self.session_maker() as session:  
            stmt = (
                select(Book)
                .options(selectinload(Book.author))
                .where(Book.id == book_id)
            )
            result = await session.execute(stmt)
            book = result.scalars().first() 
            return book
    
    async def create(self, book: Book):
        async with self.session_maker() as session:
            session.add(book)
            await session.commit()
            await session.refresh(book)
            return book
    
    async def update_by_id(self, book_id: int, new_data: dict):
        async with self.session_maker() as session:
            result = await session.execute(select(Book).where(Book.id == book_id))
            book = result.scalars().first()
            if book:
                for key, value in new_data.items():
                    setattr(book, key, value)
                await session.commit()
                await session.refresh(book)
                return book
            return None
        
    async def delete_by_id(self, book_id: int):
        async with self.session_maker() as session:
            result = await session.execute(select(Book).where(Book.id == book_id))
            book = result.scalars().first()
            if book:
                await session.delete(book)
                await session.commit()
                return True
            return False
        
    
        
    async def create_book_with_author(self, book_data: dict, author_data: dict):
        """
        Создаёт книгу и автора в одной транзакции.
        Если добавление автора упадёт, книга не сохраняется.
        """
        async with self.session_maker() as session:
                async with session.begin():  
                    author = Author(name = author_data.name)
                    session.add(author)

                    book = Book(
                        title = book_data.title,
                        genre = book_data.genre,
                        author = author
                    )
                    session.add(book)

                    await session.flush()  

                    return BookSchema(
                        id=book.id,
                        title=book.title,
                        genre=book.genre,
                        author=AuthorSchema(id=author.id, name=author.name)
                    )

          
             
           
                