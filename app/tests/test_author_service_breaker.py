import pytest
import pybreaker
from httpx import RequestError
from app.services.author_service import AuthorService
from app.repositories.dummy_repository import DummyAuthorRepo

@pytest.mark.asyncio
async def test_circuit_breaker_opens_after_5_calls(monkeypatch):
    service = AuthorService(repo=DummyAuthorRepo(), base_url="http://testserver")
    async def failing_get(path: str, *args, **kwargs):
        raise RequestError("boom", request=None)
    monkeypatch.setattr(service.client, "get", failing_get)

    for _ in range(5):
        with pytest.raises((RequestError, pybreaker.CircuitBreakerError)):
            await service.get_author_details(author_id=1)

    # 6-й вызов немедленно падает с CircuitBreakerError
    with pytest.raises(pybreaker.CircuitBreakerError):
        await service.get_author_details(author_id=1)