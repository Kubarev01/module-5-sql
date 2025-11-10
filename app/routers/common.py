from fastapi import APIRouter, HTTPException
import asyncio

from repositories.book_repository import BookRepository  
from repositories.review_repository import ReviewsRepository 
from schemas import BookSchema, ReviewSchema

router = APIRouter()

book_repo = BookRepository()
review_repo = ReviewsRepository()

@router.get("/{product_id}/details", summary="Получить детали продукта (книги и отзывы)", tags=["Общие"])
async def get_product_details(product_id: int):
 
    book_task = book_repo.get_by_id(product_id)
    reviews_task = review_repo.get_by_book_id(product_id)

    book, reviews = await asyncio.gather(book_task, reviews_task)

    if not book:
        raise HTTPException(status_code=404, detail="Book not found")

    return {
        "book": book,        
        "reviews": reviews   
    }