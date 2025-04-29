import asyncio
import sys
import os
from logging.config import fileConfig
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from alembic import context

# Добавляем корневую директорию проекта в sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Теперь импорты из app должны работать
from app.models import Base
from app.database import DATABASE_URL

# Добавляем импорт для WindowsSelectorEventLoopPolicy
from asyncio import WindowsSelectorEventLoopPolicy

# Настраиваем логирование
config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Устанавливаем DATABASE_URL в конфигурации Alembic
config.set_main_option("sqlalchemy.url", DATABASE_URL)

# Создаём движок для асинхронных операций
connectable = create_async_engine(DATABASE_URL, echo=True)

# Функция для асинхронных миграций
async def run_migrations_online() -> None:
    async with connectable.connect() as connection:
        # Настраиваем контекст для миграций
        await connection.run_sync(lambda _: context.configure(
            connection=_,  # Передаём синхронное соединение
            target_metadata=Base.metadata
        ))

        # Выполняем миграции
        async with connection.begin():
            await connection.run_sync(lambda _: context.run_migrations())

# Устанавливаем SelectorEventLoop для Windows
if sys.platform == "win32":
    asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())

# Запускаем миграции
if context.is_offline_mode():
    print("Alembic offline mode is not supported with async engines.")
else:
    asyncio.run(run_migrations_online())