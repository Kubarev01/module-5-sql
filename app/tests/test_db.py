import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.logging_config import logging

@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    SessionLocal = sessionmaker(bind=engine)
    
    session = SessionLocal()
    logging.info("Сессия создана")

    yield session

    session.close()
    logging.info("Сессия закрыта")

def test_db_session_works(db_session):
    assert hasattr(db_session, "add")
    logging.info("Тест сессии выполнен")