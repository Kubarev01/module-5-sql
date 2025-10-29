from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import async_sessionmaker
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://myuser:mypassword@db:5432/mydatabase")

engine = create_async_engine(DATABASE_URL,echo=True)
SessionLocal = async_sessionmaker(autocommit=False, autoflush=False, bind=engine)

 
async def get_db_session():
    async with SessionLocal() as session:
        yield session