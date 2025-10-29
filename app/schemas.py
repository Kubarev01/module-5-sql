from pydantic import BaseModel, Field



class BaseSchema(BaseModel):
    pass

class BookSchema(BaseSchema):

    title: str
    genre: str
    author_id: int

