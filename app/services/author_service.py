from httpx import AsyncClient, RequestError
import asyncio
import backoff
from types import SimpleNamespace
from datetime import timedelta

from aiobreaker import CircuitBreaker, CircuitBreakerError


class DummyRepo:
    async def get_by_id(self, author_id: int):
        return SimpleNamespace(details=None)


class AuthorService:
    def __init__(self, repo, base_url="http://testserver"):
        self.repo = repo
        self.client = AsyncClient(base_url=base_url)

        self.breaker = CircuitBreaker(
            fail_max=5,
           
        )

    @backoff.on_exception(backoff.expo, RequestError, max_tries=1)
    async def get_author_details(self, author_id: int):
        try:
            author = await asyncio.wait_for(self.repo.get_by_id(author_id), timeout=2.0)
        except asyncio.TimeoutError:
            print("Request timed out")
            return None


        response = await self.breaker.call_async(self.client.get, f"/authors/{author_id}")

        if response.status_code == 200:
            return response.json()
        else:
            return None