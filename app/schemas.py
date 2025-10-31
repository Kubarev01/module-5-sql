from pydantic import BaseModel, Field



class BaseSchema(BaseModel):
    pass

class BookSchema(BaseSchema):

    title: str
    genre: str


class AuthorSchema(BaseSchema):
    name: str



class ReviewSchema(BaseSchema):
    book_id: int
    content: str = Field(..., max_length=500)

    model_config = {
        "from_attributes": True  
    }