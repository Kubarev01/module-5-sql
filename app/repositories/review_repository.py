from sqlalchemy import select
from app.database.postgres_client import SessionLocal
from app.database.redis_client import redis_client as redis
from app.models import Review
from app.schemas import ReviewSchema


class ReviewsRepository:
    def __init__(self, session_maker=SessionLocal, redis_client=redis):
        self.session_maker = session_maker
        self.redis = redis_client

    async def create(self, book_id: int, content: str):
        async with self.session_maker() as session:
            review = Review(book_id=book_id, content=content)
            session.add(review)
            await session.commit()
            await session.refresh(review)

            return ReviewSchema(id=review.id, book_id=review.book_id, content=review.content)

   
    async def get_by_book_id(self, book_id: int):
        async with self.session_maker() as session:
            result = await session.execute(select(Review).where(Review.book_id == book_id))

            reviews = result.scalars().all()

            out = []
            for r in reviews:
                out.append({
                    "id": getattr(r, "id", None),
                    "book_id": getattr(r, "book_id", None),
                    "content": getattr(r, "content", None),
                })
            return out
    
    async def delete_by_id(self, review_id: int) -> bool:
        async with self.session_maker() as session:
            result = await session.execute(
                select(Review).where(Review.id == review_id)
            )
            review = result.scalars().first()
            if review:
                await session.delete(review)
                await session.commit()
                return True
            return False

    async def update_content(self, review_id: int, new_content: str):
        async with self.session_maker() as session:
            result = await session.execute(
                select(Review).where(Review.id == review_id)
            )
            review = result.scalars().first()
            if review:
                review.content = new_content
                await session.commit()
                await session.refresh(review)
                return ReviewSchema(id=review.id, book_id=review.book_id, content=review.content)
            return None