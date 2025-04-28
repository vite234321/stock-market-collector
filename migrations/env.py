import os
import sys
import asyncio

# Добавляем корневую директорию проекта в sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from logging.config import fileConfig
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import pool
from alembic import context

# Импортируем модели
from app.models import Base

# Настройка логирования
config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata

# Получаем URL базы данных
url = config.get_main_option("sqlalchemy.url")

# Создаём асинхронный движок
engine = create_async_engine(url, poolclass=pool.NullPool)

def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()

async def run_migrations_online() -> None:
    """Run migrations in 'online' mode with async engine."""
    async with engine.connect() as connection:
        await connection.run_sync(do_run_migrations)

def do_run_migrations(connection):
    """Helper function to run migrations synchronously within async context."""
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()

# Запускаем миграции
if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())