from httpx import AsyncClient, RequestError, ReadTimeout
import asyncio
import backoff
from types import SimpleNamespace

from aiobreaker import CircuitBreaker, CircuitBreakerError


class DummyRepo:
    async def get_by_id(self, author_id: int):
        # просто заглушка, чтобы не мешала логике breaker’а
        return SimpleNamespace(id=author_id, name=None)
    

class AuthorService:
    def __init__(self, repo, base_url="http://testserver"):
        self.repo = repo
        self.client = AsyncClient(base_url=base_url)

        self.breaker = CircuitBreaker(
            fail_max=5,
        
        )

    @backoff.on_exception(backoff.expo, (RequestError, ReadTimeout), max_tries=3)
    async def get_author_details(self, author_id: int):
        try:
            author = await asyncio.wait_for(self.repo.get_by_id(author_id), timeout=2.0)
        except asyncio.TimeoutError:
            print("Repo timed out")
            return None

        try:
            response = await self.breaker.call_async(
                self.client.get,
                f"/authors/{author_id}",
            )
        except CircuitBreakerError:

            return {
                "id": author_id,
                "name": "Default Author",
            }


        if response.status_code == 200:
            return response.json()
        else:
            return None