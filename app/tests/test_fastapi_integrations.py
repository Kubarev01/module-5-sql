from fastapi.testclient import TestClient
from main import app 
from database.postgres_client import get_db_session
import pytest
from httpx import AsyncClient
from httpx._transports.asgi import ASGITransport

book_id = 35

import fakeredis.aioredis as fakeredis
import app.repositories.book_repository as br

import pytest

@pytest.fixture()
def _fake_redis(monkeypatch):
    fake = fakeredis.FakeRedis()
    monkeypatch.setattr(br, "redis", fake)
    return fake

class FakeDbSession:
    async def execute(self, sql, *args, **kwargs):
        class FakeResult:
            def mappings(self):
                return self
            def all(self):
                return [{"id": book_id, "title": "Fake Book"}]
        return FakeResult()

    async def commit(self):
        return None

async def fake_get_db_session():
    yield FakeDbSession()



def test_get_books():
    app.dependency_overrides[get_db_session] = fake_get_db_session
    client = TestClient(app)

    response = client.get(f"/books/{book_id}",)  
    assert response.status_code == 200 
    app.dependency_overrides.pop(get_db_session, None)



@pytest.mark.asyncio
async def test_create_book():
    app.dependency_overrides[get_db_session] = fake_get_db_session
    

    new_book_data = {
        "title": "New Book",
        "genre": "Fiction",
        "author_id": 1
    }
    transport = ASGITransport(app=app) # обернул приложение в транспорт
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/books/", json=new_book_data)  
        assert response.status_code == 201  
        app.dependency_overrides.pop(get_db_session, None)