from fastapi import HTTPException, status, Depends, APIRouter

from repositories.author_repository import AuthorRepository
from database.postgres_client import get_db_session
from schemas import AuthorSchema
from database.redis_client import redis_client as redis
from services.author_service import AuthorService

repo = AuthorRepository()
service = AuthorService(repo,base_url="http://localhost:8000")
router = APIRouter(
    prefix="/authors",
    tags=["авторы"]
)

@router.post('/', summary = "Добавление нового автора",status_code=status.HTTP_201_CREATED)
async def create_author(new_author:AuthorSchema,db = Depends(get_db_session)):
    created_author = await service.create_author({"name": new_author.name})

    if not created_author:
        raise HTTPException(status_code=400, detail="Ошибка при создании автора")
    return created_author

@router.get('/{author_id}', summary="Получить автора по ID")
async def get_author(author_id: int, db = Depends(get_db_session)):
    author =  await service.get_author_details(author_id)
    if not author:
        raise HTTPException(status_code=404, detail="Автор не найден")
    return author

@router.put('/{author_id}', summary="Обновить информацию об авторе")
async def update_author(author_id: int, updated_author: AuthorSchema, db = Depends(get_db_session)):
    author = await service.update(author_id, {"name": updated_author.name})
    if not author:
        raise HTTPException(status_code=404, detail="Автор не найден")
    return author

@router.delete('/{author_id}', summary="Удалить автора по ID")
async def delete_author(author_id: int, db = Depends(get_db_session)):
    success = await service.delete(author_id)
    if not success:
        raise HTTPException(status_code=404, detail="Автор не найден")
    return {"detail": "Автор успешно удален"}

