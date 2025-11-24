import asyncio
import backoff
from httpx import AsyncClient, RequestError, ReadTimeout
from types import SimpleNamespace
from app.repositories.author_repository import AuthorRepository

# важно: тест ждёт pybreaker.CircuitBreakerError
import pybreaker
from aiobreaker import CircuitBreaker, CircuitBreakerError as AioCBError


class DummyRepo:
    async def get_by_id(self, author_id: int):
        # простая заглушка, чтобы не мешала логике breaker’а
        return SimpleNamespace(id=author_id, name=None)


class AuthorService:
    _semaphore = asyncio.Semaphore(5)

    def __init__(self, repo: AuthorRepository, base_url: str = "http://localhost:8000"):
        self.repo = repo
        # даём таймаут клиенту, чтобы избежать зависаний
        self.client = AsyncClient(base_url=base_url, timeout=2.0)
        self.breaker = CircuitBreaker(fail_max=5)

    @backoff.on_exception(backoff.expo, (RequestError, ReadTimeout), max_tries=3)
    async def get_author_details(self, author_id: int):
        # ограничим ожидание репозитория
        try:
            author = await asyncio.wait_for(self.repo.get_by_id(author_id), timeout=2.0)
        except asyncio.TimeoutError:
            return None

        async with self._semaphore:
            # НЕ перехватываем RequestError — тест должен его видеть
            try:
                author_resp = await self.breaker.call_async(self.client.get, f"/authors/{author_id}")
            except AioCBError as e:
                # конвертация, чтобы тест поймал именно pybreaker.CircuitBreakerError
                raise pybreaker.CircuitBreakerError(str(e)) from e

            author_name = author_resp.json().get("name")

            # отзывы — best effort: ошибка не должна ронять основной сценарий
            reviews = []
            try:
                reviews_resp = await self.client.get(f"/authors/{author_id}/reviews")
                reviews = reviews_resp.json()
            except (RequestError, ReadTimeout):
                reviews = []

            return SimpleNamespace(id=author.id, name=author_name, reviews=reviews)

    async def create_author(self, author_data: dict):
        return await self.repo.create(author_data)