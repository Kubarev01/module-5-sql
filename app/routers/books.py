from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


try:
    from database.redis_client import redis_client as redis  # type: ignore
except Exception:
    redis = None  # type: ignore

# мягкий импорт метрик — не ломаемся, если модуля нет
try:
    from metrics import book_created_counter  # type: ignore
except Exception:  # pragma: no cover
    class _DummyCounter:
        def add(self, *args, **kwargs):
            pass
    book_created_counter = _DummyCounter()  # type: ignore

from database.postgres_client import get_db_session
from repositories.book_repository import BookRepository
from schemas import BookSchema, AuthorSchema

repo = BookRepository()
router = APIRouter(prefix="/books", tags=["Книги"])


# -------- Отчёты --------
@router.get("/report/book-authors")
async def books_authors_report(db: AsyncSession = Depends(get_db_session)):
    sql = text(
        """
        SELECT
            b.id   AS book_id,
            b.title,
            b.genre,
            a.id   AS author_id,
            a.name AS author_name
        FROM books b
        LEFT JOIN authors a ON b.author_id = a.id
        ORDER BY a.name, b.genre
        """
    )
    result = await db.execute(sql)
    rows = result.mappings().all()
    return {"report": rows}


# -------- CRUD --------
@router.post("/", summary="Добавление новой книги", status_code=status.HTTP_201_CREATED)
async def create_book(
    new_book: BookSchema,
    db: AsyncSession = Depends(get_db_session),
):
    created = await repo.create(
        {
            "title": new_book.title,
            "genre": new_book.genre,
            "author_id": new_book.author_id,
        },
        session=db,
    )

    if not created:
        raise HTTPException(status_code=400, detail="Ошибка при создании книги")

    # метрика — мягкая, не влияет на ответ
    book_created_counter.add(1, attributes={"service": "book-service"})  # type: ignore
    return created


@router.post(
    "/with-author",
    summary="Создание книги с автором",
    status_code=status.HTTP_201_CREATED,
)
async def create_book_with_author(
    new_book: BookSchema,
    new_author: AuthorSchema,
    db: AsyncSession = Depends(get_db_session),
):
    created = await repo.create_book_with_author(
        book_data=new_book, author_data=new_author, session=db
    )

    if not created:
        raise HTTPException(status_code=400, detail="Ошибка при создании книги")

    return {
        "status": "success",
        "msg": "Книга с автором добавлена",
        "book": created,
        "author": new_author,
    }


@router.get("/{book_id}", summary="Просмотр книги")
async def get_book(
    book_id: int,
    db: AsyncSession = Depends(get_db_session),
):
    book = await repo.get_by_id(book_id, session=db)
    if book:
        return book
    raise HTTPException(status_code=404, detail="Книга не найдена")


@router.put("/{book_id}", summary="Изменить книгу")
async def update_book(
    book_id: int,
    new_book: BookSchema,
    db: AsyncSession = Depends(get_db_session),
):
    lock_key = f"lock:book:{book_id}"

    async def _do_update():
        updated = await repo.update_by_id(
            book_id, new_data=new_book.dict(), session=db
        )
        if not updated:
            raise HTTPException(status_code=404, detail="Книга не найдена")
        # инвалидация кэша в репозитории уже делается; публикация — best-effort
        if redis:
            try:
                await redis.publish("cache:invalidate", str(book_id))  # type: ignore
            except Exception:
                pass
        return {"status": "success", "msg": "Книга обновлена", "book": updated}

    # если Redis есть — используем распределённый лок, иначе просто обновляем
    if redis:
        try:
            async with redis.lock(lock_key, timeout=10):  # type: ignore
                return await _do_update()
        except Exception:
            # в случае проблем с Redis — не блокируем основной сценарий
            return await _do_update()
    else:
        return await _do_update()


@router.delete("/{book_id}", summary="Удалить книгу")
async def delete_book(
    book_id: int,
    db: AsyncSession = Depends(get_db_session),
):
    deleted = await repo.delete_by_id(book_id, session=db)
    if deleted:
        return {"status": "success", "msg": "Книга удалена"}
    raise HTTPException(status_code=404, detail="Книга не найдена")