
from httpx import AsyncClient

class AuthorService:
    def __init__(self, repo, base_url: str):
        self.repo = repo
        self.client = AsyncClient(base_url=base_url)

    async def get_authors(self):
        # Пример метода для получения авторов
        response = await self.client.get("/authors")
        response.raise_for_status()
        return response.json()

    async def close(self):
        # Закрываем клиент, чтобы освободить соединения
        await self.client.aclose()
