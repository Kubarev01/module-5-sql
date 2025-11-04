# tests/test_book_service.py
import pytest
from services.book_service import BookService


class FakeBookRepository:
    def create(self, data):
        raise NotImplementedError
    def get_by_id(self, book_id):
        raise NotImplementedError
    def update_by_id(self, book_id, new_data):
        raise NotImplementedError
    def delete_by_id(self, book_id):
        raise NotImplementedError
    def create_book_with_author(self, book_data, author_data):
        raise NotImplementedError


class FakeRedis:
    def publish(self, channel, message):
        return None
    
@pytest.fixture
def fake_redis():
    return FakeRedis()

def test_create_book(mocker, fake_redis):
    repo = FakeBookRepository()
    mocker.patch.object(repo, "create", return_value={"id": 1, "title": "Mocked Book"})

    service = BookService(repo, fake_redis)
    result = service.create({"title": "Any Book"})  
    assert result["title"] == "Mocked Book"
    assert result["id"] == 1

def test_get_book(mocker):
    repo = FakeBookRepository()
    mocker.patch.object(repo, "get_by_id", return_value={"id": 1, "title": "Mocked Book"})

    service = BookService(repo, None)
    book = service.get_by_id(1)  
    assert book["id"] == 1
    assert book["title"] == "Mocked Book"

def test_delete_book(mocker):
    repo = FakeBookRepository()
    mocker.patch.object(repo, "delete_by_id", return_value=True)

    service = BookService(repo, None)
    result = service.delete_by_id(1)
    assert result is True

def test_update_book(mocker, fake_redis):
    repo = FakeBookRepository()
    mocker.patch.object(repo, "update_by_id", return_value={"id": 1, "title": "Updated Book"})

    service = BookService(repo, fake_redis)
    updated_book = service.update_by_id(1, {"title": "New Title"})
    assert updated_book["title"] == "Updated Book"
    assert updated_book["id"] == 1