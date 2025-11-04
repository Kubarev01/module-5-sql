
from httpx import AsyncClient
import asyncio
class AuthorService:
    def __init__(self, repo, base_url: str):
        self.repo = repo
        self.client = AsyncClient(base_url=base_url)

   
    async def get_author_details(self, author_id: int):
        try:
            author = await asyncio.wait_for( self.repo.get_by_id(author_id), timeout=2.0)
        except asyncio.TimeoutError:
            print("Request timed out")
            return None
            

        response = await self.client.get(f"/authors/{author_id}/details")
        if response.status_code == 200:
            details = response.json()
            author.details = details
        return author