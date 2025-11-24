import sqlalchemy
from sqlalchemy import Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column,relationship



class Base(DeclarativeBase):
    pass

class Book(Base):
    __tablename__ = 'books'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title: Mapped[str] = mapped_column(String, nullable=False)
    genre : Mapped[str] = mapped_column(String, nullable=False)
    author_id : Mapped[int] = mapped_column(Integer, sqlalchemy.ForeignKey('authors.id'), nullable=True)
    author = relationship("Author", back_populates="books")

class Author(Base):
    __tablename__ = 'authors'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    books = relationship("Book", back_populates="author")
   

class Review(Base):
    __tablename__ = 'reviews'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    book_id: Mapped[int] = mapped_column(Integer, sqlalchemy.ForeignKey('books.id'), nullable=False)
    content: Mapped[str] = mapped_column(String(500), nullable=False)
    book = relationship("Book")
    

class Inventory(Base):
    __tablename__ = 'inventory_updates'

    product_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    stock: Mapped[int] = mapped_column(Integer, default=0)

   
