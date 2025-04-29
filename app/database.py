# app/database.py
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
import os
import logging

# Настройка логирования
logger = logging.getLogger(__name__)

# Получаем строку подключения из переменной окружения
DATABASE_URL = os.getenv("DATABASE_URL")
logger.info(f"Исходный DATABASE_URL: {DATABASE_URL}")

# Supabase использует формат postgres://, заменяем его на postgresql+asyncpg://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)

logger.info(f"Обновлённый DATABASE_URL: {DATABASE_URL}")

# Создаём асинхронный движок
try:
    engine = create_async_engine(DATABASE_URL, echo=True)
except Exception as e:
    logger.error(f"Ошибка создания движка SQLAlchemy: {str(e)}")
    raise

# Создаём фабрику сессий
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)