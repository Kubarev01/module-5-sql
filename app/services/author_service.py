
from httpx import AsyncClient, RequestError
import asyncio
import backoff

class AuthorService:
    def __init__(self, repo, base_url: str):
        self.repo = repo
        self.client = AsyncClient(base_url=base_url)

    @backoff.on_exception(backoff.expo, RequestError, max_tries=3)
    async def get_author_details(self, author_id: int):
        try:
            author = await asyncio.wait_for(self.repo.get_by_id(author_id), timeout=2.0)
        except asyncio.TimeoutError:
            print("Request timed out")
            return None
            
        response = await self.client.get(f"/authors/{author_id}/details")
        if response.status_code == 200:
            details = response.json()
            author.details = details
        return author
    
    async def create_author(self, data: dict):
        return await self.repo.create(data)
    
    async def get_by_id(self, author_id: int):
        return await self.repo.get_by_id(author_id)

    async def update(self, author_id: int, new_data: dict):
        return await self.repo.update_by_id(author_id, new_data)
    
    async def delete(self, author_id: int):
        return await self.repo.delete_by_id(author_id)
    