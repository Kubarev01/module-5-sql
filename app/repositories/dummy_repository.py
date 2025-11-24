from types import SimpleNamespace

class DummyAuthorRepo:
    '''
    фейковый репозиторий для экспериментов
    '''
    async def get_by_id(self, author_id: int):
        return SimpleNamespace(id = author_id, name = None)

