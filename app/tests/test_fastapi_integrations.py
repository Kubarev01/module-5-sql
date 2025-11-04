from fastapi.testclient import TestClient
from main import app 
from database.postgres_client import get_db_session


book_id = 35
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