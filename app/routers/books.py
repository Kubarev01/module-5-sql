from fastapi import HTTPException, status, Depends, APIRouter

from database.redis_client import redis_client as redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from repositories.book_repository import BookRepository
from database.postgres_client import get_db_session
from schemas import BookSchema, AuthorSchema
from models import Book, Author
from database.redis_client import redis_client as redis
from services.book_service import BookService

repo = BookRepository()
service = BookService(repo, redis)
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
    created_book = await service.create({"title": new_book.title,
                                       "genre": new_book.genre,
                                       "author_id": new_book.author_id})


    if not created_book:
        raise HTTPException(status_code=400, detail="Ошибка при создании книги")
    return created_book

@router.post('/with-author', summary="Создание книги с автором", status_code=status.HTTP_201_CREATED)
async def create_book_with_author(new_book: BookSchema, new_author: AuthorSchema, db: AsyncSession = Depends(get_db_session)):
    created_book = await service.create_book_with_author(
        book_data = new_book,
        author_data = new_author
    )

    await db.commit()
 

    if not created_book:
        raise HTTPException(status_code=400, detail="Ошибка при создании книги")
    return {"status":"success","msg":"Книга с автором добавлена","book": created_book, "author": new_author}

@router.get("/{book_id}",summary = "Просмотр книг")
async def get_book(book_id: int, db: AsyncSession = Depends(get_db_session)):
    
    book = await service.get_by_id(book_id)

    if book:
        return book
    raise HTTPException(status_code=404, detail="Книга не найдена")
    

@router.put("/{book_id}", summary="Изменить книгу")
async def update_book(book_id: int, new_book:BookSchema, db: AsyncSession = Depends(get_db_session)):
    lock_key = f"lock:book:{book_id}"
    async with redis.lock(lock_key, timeout=10):
        updated_book = await service.update_by_id(book_id, new_data=new_book.dict())

        await db.commit()
    

        if updated_book:
            await redis.publish("cache:invalidate", str(book_id))
            return {"status":"success","msg":"Книга обновлена","book": updated_book}
        raise HTTPException(status_code=404, detail="Книга не найдена")
        


@router.delete("/{book_id}", summary="Удалить книгу")
async def delete_book(book_id:int, db: AsyncSession = Depends(get_db_session)):
    deleted = await repo.delete_by_id(book_id)

    await db.commit()

    if deleted:
        return {"status":"success","msg":"Книга удалена"}
    raise HTTPException(status_code=404, detail="Книга не найдена")