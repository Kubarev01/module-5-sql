import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from testcontainers.postgres import PostgresContainer
from models import Book, Base
from services.book_service import BookService

class FakeRedis:
    def publish(self, *args, **kwargs):
        return None

    def lock(self, *args, **kwargs):
        class DummyLock:
            async def __aenter__(self):
                return self
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass
        return DummyLock()

@pytest.fixture
def fake_redis():
    return FakeRedis()

@pytest.fixture
def fake_book_service(fake_redis):
    from repositories.book_repository import BookRepository
    repo = BookRepository() 
    service = BookService(repo, fake_redis)
    return service

def insert_and_get_book(session, title: str, genre: str):
    book = Book(title=title, genre=genre)
    session.add(book)
    session.commit()
    return session.query(Book).filter_by(title=title).first()

@pytest.mark.parametrize(
    "book_title, book_genre",
    [
        ("Book1", "Genre1"),
        ("Book2", "Genre2"),
    ]
)
def test_postgres_e2e(book_title, book_genre):
    with PostgresContainer("postgres:16") as pg:
        engine = create_engine(pg.get_connection_url())
        Session = sessionmaker(bind=engine)

        Base.metadata.create_all(engine)

        session = Session()

        book = insert_and_get_book(session, book_title, book_genre)
        assert book is not None
        assert book.title == book_title
        assert book.genre == book_genre

        session.close()