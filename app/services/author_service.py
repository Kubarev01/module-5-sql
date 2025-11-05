from httpx import AsyncClient, RequestError, ReadTimeout
import asyncio
import backoff
from types import SimpleNamespace
from repositories.author_repository import AuthorRepository
from aiobreaker import CircuitBreaker, CircuitBreakerError
import aiohttp
class DummyRepo:
    async def get_by_id(self, author_id: int):
        # просто заглушка, чтобы не мешала логике breaker’а
        return SimpleNamespace(id=author_id, name=None)
    

class AuthorService:
    _semaphore = asyncio.Semaphore(5)
    def __init__(self, repo: AuthorRepository, base_url="http://localhost:8000"):
        self.repo = repo
        self.client = AsyncClient(base_url=base_url)

        self.breaker = CircuitBreaker(
            fail_max=5,
        
        )

    @backoff.on_exception(backoff.expo, (RequestError, ReadTimeout), max_tries=3)
    async def get_author_details(self, author_id: int):
        try:
            with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=2.0)):
                author = await self.repo.get_by_id(author_id)
        except asyncio.TimeoutError:
            print("Repo timed out")
            return None
        
        async with self._semaphore:
            try:
                author_call, review_call = await asyncio.gather(
                    self.breaker.call_async(
                        self.client.get,
                        f"/authors/{author_id}",
                    ),
                    self.client.get(f"/authors/{author_id}/reviews"),
                )
                responses = await asyncio.gather(
                author_call,
                review_call,
                return_exceptions=True,  
            )
            except CircuitBreakerError as e:
                
                return print(f"Circuit breaker is open: {e}")
           
            

            author_response, review_response = responses
            if isinstance(author_response, Exception):
                print(f"Author service call failed: {author_response}")
                return None
            if isinstance(review_response, Exception):
                print(f"Review service call failed: {review_response}")
                return None

            return SimpleNamespace(
                id=author.id,
                name=author_response.json().get("name"),
                reviews=review_response.json(),
            )
        
    async def create_author(self, author_data: dict):
        return await self.repo.create(author_data)