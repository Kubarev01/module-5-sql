from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

def test_create_book_validation_error():
    response = client.post("/books", json={"title": 123})

    assert response.status_code == 422
