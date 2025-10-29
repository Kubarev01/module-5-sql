from fastapi import HTTPException, status, Depends, APIRouter

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
import asyncio
from repositories.book_repository import BookRepository
from database import get_db_session
from schemas import BookSchema, AuthorSchema
from models import Book, Author


router = APIRouter(
    prefix="/books",
    tags=["Книги"]
)

#отчеты

@router.get('report/book-authors')
async def books_authors_report(db: AsyncSession = Depends(get_db_session)):
    sql = text("""
        SELECT
            b.id AS book_id,
            b.title,
            b.genre,
            a.id AS author_id,
            a.name AS author_name
        FROM books b
        LEFT JOIN authors a ON b.author_id = a.id
        ORDER BY a.name, b.genre
    """)


    result = await db.execute(sql)
    
    rows = result.mappings().all()  

    return {"report": rows}
   



#CRUD операции с использованием репозитория

@router.post('/', summary = "Добавление новой книги",status_code=status.HTTP_201_CREATED)
async def create_book(new_book:BookSchema,db: AsyncSession = Depends(get_db_session)):
    repo = BookRepository()
    created_book = await repo.create(Book(title=new_book.title, genre=new_book.genre, author_id=new_book.author_id))
    if not created_book:
        raise HTTPException(status_code=400, detail="Ошибка при создании книги")
    return created_book

@router.post('/with-author', summary="Создание книги с автором", status_code=status.HTTP_201_CREATED)
async def create_book_with_author(new_book: BookSchema, new_author: AuthorSchema, db: AsyncSession = Depends(get_db_session)):
    repo = BookRepository()
    created_book = await repo.create_book_with_author(
        book_data = new_book,
        author_data = new_author
    )
    if not created_book:
        raise HTTPException(status_code=400, detail="Ошибка при создании книги")
    return {"status":"success","msg":"Книга с автором добавлена","book": created_book, "author": new_author}

@router.get("/{book_id}",summary = "Просмотр книг")
async def get_book(book_id: int, db: AsyncSession = Depends(get_db_session)):
    repo = BookRepository()
    book = await repo.get_by_id(book_id)
    if book:
        return book
    raise HTTPException(status_code=404, detail="Книга не найдена")
    

@router.put("/{book_id}", summary="Изменить книгу")
async def update_book(book_id: int, new_book:BookSchema, db: AsyncSession = Depends(get_db_session)):
    repo = BookRepository()
    updated_book = await repo.update_by_id(book_id, new_data=new_book.dict())
    if updated_book:
        return {"status":"success","msg":"Книга обновлена","book": updated_book}
    raise HTTPException(status_code=404, detail="Книга не найдена")
    


@router.delete("/{book_id}", summary="Удалить книгу")
async def delete_book(book_id:int, db: AsyncSession = Depends(get_db_session)):
    repo = BookRepository()
    deleted = await repo.delete_by_id(book_id)
    if deleted:
        return {"status":"success","msg":"Книга удалена"}
    raise HTTPException(status_code=404, detail="Книга не найдена")