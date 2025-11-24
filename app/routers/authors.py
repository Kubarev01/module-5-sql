from fastapi import HTTPException, status, APIRouter
from app.repositories.author_repository import AuthorRepository
from app.schemas import AuthorSchema
from app.services.author_service import AuthorService

repo = AuthorRepository()
service = AuthorService(repo,base_url="http://localhost:8000")
router = APIRouter(
    prefix="/authors",
    tags=["авторы"]
)

@router.post('/', summary = "Добавление нового автора",status_code=status.HTTP_201_CREATED)
async def post_author(new_author:AuthorSchema):
    created_author = await service.create_author({"name": new_author.name})

    if not created_author:
        raise HTTPException(status_code=400, detail="Ошибка при создании автора")
    return created_author

@router.get('/{author_id}', summary="Получить автора по ID")
async def get_author(author_id: int):
    author =  await service.get_author_details(author_id)
    if not author:
        raise HTTPException(status_code=404, detail="Автор не найден")
    return author

@router.get('/{author_id}/reviews', summary="Получить отзывы об авторе по ID")
async def get_author_reviews(author_id: int):
    author =  await service.get_author_details(author_id)
    if not author:
        raise HTTPException(status_code=404, detail="Автор не найден")
    return author.reviews

