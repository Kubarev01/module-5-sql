# alembic/env.py
from logging.config import fileConfig
import os
import sys
import asyncio

from alembic import context
from sqlalchemy import engine_from_config, pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

# allow importing project modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# импортируй свою metadata (у тебя в проекте: models.Base)
from models import Base, Book, Author

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def get_url() -> str:
    # приоритет - переменная окружения, иначе алембиковский sqlalchemy.url
    return os.getenv("DATABASE_URL") or config.get_main_option("sqlalchemy.url")


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    """
    Функция, которую Alembic ожидает выполнять синхронно.
    При async-движке она будет вызвана через connection.run_sync(...)
    """
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online_async(url: str) -> None:
    """Async-ветка: создаём AsyncEngine и вызываем do_run_migrations через run_sync."""
    connectable = create_async_engine(
        url,
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        # run_sync выполнит do_run_migrations в синхронном контексте, как требуется Alembic
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online_sync(url: str) -> None:
    """Sync-ветка: ведёт себя как старый env.py (engine_from_config)."""
    configuration = config.get_section(config.config_ini_section)
    configuration["sqlalchemy.url"] = url
    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        do_run_migrations(connection)


def run_migrations_online() -> None:
    """Выбираем ветку (async или sync) в зависимости от URL."""
    url = get_url()
    if url.startswith("postgresql+asyncpg"):
        # запускаем async loop
        asyncio.run(run_migrations_online_async(url))
    else:
        run_migrations_online_sync(url)


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()