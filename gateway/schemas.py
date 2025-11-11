from pydantic import BaseModel


class BaseSchema(BaseModel):
    pass


class BookSchema(BaseSchema):
    title: str
    genre: str
    author_id: int | None = None