from sqlalchemy import select
from database import SessionLocal
from models import Book

class BookRepository:
    def __init__(self, session_maker=SessionLocal):
        self.session_maker = session_maker

    async def get_by_id(self, book_id: int):
        async with self.session_maker() as session:
            result = await session.execute(select(Book).where(Book.id == book_id))
            return result.scalars().first()
    
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