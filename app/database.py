import logging
import os
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from dotenv import load_dotenv
load_dotenv()  # Загружаем переменные из .env
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL не установлен")
logger.info(f"Исходный DATABASE_URL: {DATABASE_URL}")

# Заменяем схему postgres:// на postgresql+psycopg://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg://")
logger.info(f"Обновлённый DATABASE_URL: {DATABASE_URL}")

# Создаём движок SQLAlchemy
try:
    engine = create_async_engine(
        DATABASE_URL,
        echo=True,  # Включаем отладку SQL-запросов
        pool_pre_ping=True  # Проверяем соединения перед использованием
    )
    logger.info("Движок SQLAlchemy для коннектора создан успешно")
except Exception as e:
    logger.error(f"Ошибка создания движка SQLAlchemy: {e}")
    raise

# Создаём фабрику сессий
async_session = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False
)
logger.info("Фабрика сессий SQLAlchemy создана успешно")

# Базовый класс для моделей
class Base(DeclarativeBase):
    pass

# Асинхронный контекстный менеджер для сессии базы данных
async def get_db() -> AsyncSession:
    logger.info("Открытие сессии базы данных...")
    async with async_session() as session:
        try:
            logger.info("Сессия базы данных открыта")
            yield session
            logger.info("Сессия базы данных закрыта")
        except Exception as e:
            logger.error(f"Ошибка в сессии базы данных: {e}")
            raise
        finally:
            await session.close()