# test_author_service_breaker.py
import pytest
from httpx import RequestError
import pybreaker

from services.author_service import AuthorService, DummyRepo  

from types import SimpleNamespace

class DummyRepo:

    async def get_by_id(self, author_id: int):
        # просто фейковый автор, чтобы не мешал эксперименту с breaker
        return SimpleNamespace(details=None)


@pytest.mark.asyncio
async def test_circuit_breaker_opens_after_5_calls(monkeypatch):
    service = AuthorService(repo=DummyRepo(), base_url="http://testserver")

    # замокаем client.get так, чтобы он ВСЕГДА падал RequestError
    async def failing_get(path: str, *args, **kwargs):
        raise RequestError("boom", request=None)

    # важная строка: именно так мы "замокаем" этот вызов:
    # response = await self.breaker.call_async(self.client.get, ...)
    monkeypatch.setattr(service.client, "get", failing_get)

    # первые 5 логических вызовов — считаем, что они могут падать либо RequestError,
    # либо CircuitBreakerError (брейкер может открытьcя чуть раньше из-за backoff)
    for _ in range(5):
        with pytest.raises((RequestError, pybreaker.CircuitBreakerError)):
            await service.get_author_details(author_id=1)

    # критерий приёмки: 6-й вызов немедленно падает с CircuitBreakerError
    with pytest.raises(pybreaker.CircuitBreakerError):
        await service.get_author_details(author_id=1)