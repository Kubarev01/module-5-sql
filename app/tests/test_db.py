import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    SessionLocal = sessionmaker(bind=engine)
    
    session = SessionLocal()
    print("Сессия создана")

    yield session

    session.close()
    print("Сессия закрыта")

def test_db_session_works(db_session):
    # проверяем, что сессия это объект с методом add
    assert hasattr(db_session, "add")
    print("Тест сессии выполнен")