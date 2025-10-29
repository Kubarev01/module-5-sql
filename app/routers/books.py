from fastapi import HTTPException, status, Depends, APIRouter
from sqlalchemy.orm import Session
import asyncio
from repositories.book_repository import BookRepository
from database import get_db_session
from schemas import BookSchema
from models import Book


router = APIRouter(
    prefix="/books",
    tags=["Книги"]
)

@router.post('/', summary = "Добавление новой книги",status_code=status.HTTP_201_CREATED)
async def create_book(new_book:BookSchema,db: Session = Depends(get_db_session)):
    repo = BookRepository()
    created_book = await repo.create(Book(title=new_book.title, genre=new_book.genre, author_id=new_book.author_id))
    if not created_book:
        raise HTTPException(status_code=400, detail="Ошибка при создании книги")
    return created_book

@router.get("/{book_id}",summary = "Просмотр книг")
async def get_book(book_id: int, db: Session = Depends(get_db_session)):
    repo = BookRepository()
    book = await repo.get_by_id(book_id)
    if book:
        return book
    raise HTTPException(status_code=404, detail="Книга не найдена")
    

@router.put("/{book_id}", summary="Изменить книгу")
async def update_book(book_id: int, new_book:BookSchema):
    repo = BookRepository()
    updated_book = await repo.update_by_id(book_id, new_data=new_book.dict())
    if updated_book:
        return {"status":"success","msg":"Книга обновлена","book": updated_book}
    raise HTTPException(status_code=404, detail="Книга не найдена")
    


@router.delete("/{book_id}", summary="Удалить книгу")
async def delete_book(book_id:int):
    repo = BookRepository()
    deleted = await repo.delete_by_id(book_id)
    if deleted:
        return {"status":"success","msg":"Книга удалена"}
    raise HTTPException(status_code=404, detail="Книга не найдена")