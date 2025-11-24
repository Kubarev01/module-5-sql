from fastapi import HTTPException, APIRouter
from app.repositories.review_repository import ReviewsRepository

repo = ReviewsRepository()

router = APIRouter(
    prefix="/reviews",
    tags=["Отзывы"]
)

@router.post("/",summary="Создать отзыв")
async def create_review(book_id: int, content: str):
    new_review = await repo.create(book_id=book_id, content=content)
    if not new_review:
        raise HTTPException(status_code=400, detail="Ошибка при создании отзыва")
    return new_review

@router.get("/book/{book_id}", summary="Получить отзывы по книге")
async def get_reviews_by_book(book_id: int):
    reviews = await repo.get_by_book_id(book_id=book_id)
    return reviews


@router.delete("/{review_id}", summary="Удалить отзыв по ID")
async def delete_review(review_id: int):
    success = await repo.delete_by_id(review_id=review_id)
    if not success:
        raise HTTPException(status_code=404, detail="Отзыв не найден")
    return {"detail": "Отзыв успешно удален"}


@router.put("/{review_id}", summary="Обновить содержание отзыва")
async def update_review(review_id: int, new_content: str):
    updated_review = await repo.update_content(review_id=review_id, new_content=new_content)
    if not updated_review:
        raise HTTPException(status_code=404, detail="Отзыв не найден")
    return updated_review



