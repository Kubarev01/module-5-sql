from httpx import AsyncClient, RequestError, ReadTimeout
import asyncio
import backoff
from types import SimpleNamespace
from repositories.author_repository import AuthorRepository
from aiobreaker import CircuitBreaker


class DummyRepo:
    async def get_by_id(self, author_id: int):
        # простая заглушка, чтобы не мешала логике breaker’а
        return SimpleNamespace(id=author_id, name=None)


class AuthorService:
    _semaphore = asyncio.Semaphore(5)

    def __init__(self, repo: AuthorRepository, base_url: str = "http://localhost:8000"):
        self.repo = repo
        # таймаут даём клиенту сразу, чтобы не плодить доп. сессии
        self.client = AsyncClient(base_url=base_url, timeout=2.0)
        self.breaker = CircuitBreaker(fail_max=5)

    @backoff.on_exception(backoff.expo, (RequestError, ReadTimeout), max_tries=3)
    async def get_author_details(self, author_id: int):
        # ждём репозиторий ограниченное время, иначе вернём None
        try:
            author = await asyncio.wait_for(self.repo.get_by_id(author_id), timeout=2.0)
        except asyncio.TimeoutError:
            return None

        async with self._semaphore:
            # ВАЖНО: не перехватываем RequestError/CircuitBreakerError —
            # тест ожидает, что они поднимутся наружу.
            author_resp = await self.breaker.call_async(self.client.get, f"/authors/{author_id}")
            author_name = author_resp.json().get("name")

            # Отзывы — best-effort: ошибка тут не должна ломать основной сценарий
            reviews = []
            try:
                reviews_resp = await self.client.get(f"/authors/{author_id}/reviews")
                reviews = reviews_resp.json()
            except (RequestError, ReadTimeout):
                reviews = []

            return SimpleNamespace(id=author.id, name=author_name, reviews=reviews)

    async def create_author(self, author_data: dict):
        return await self.repo.create(author_data)