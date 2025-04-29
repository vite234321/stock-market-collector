# app/database.py
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
import os

# Получаем строку подключения из переменной окружения
DATABASE_URL = os.getenv("DATABASE_URL")

# Supabase использует формат postgres://, заменяем его на postgresql+asyncpg://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)

# Создаём асинхронный движок
engine = create_async_engine(DATABASE_URL, echo=True)

# Создаём фабрику сессий
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)