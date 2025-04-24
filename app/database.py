import logging
import os
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Получаем DATABASE_URL и заменяем схему для asyncpg
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    logger.error("Переменная окружения DATABASE_URL не установлена")
    raise ValueError("DATABASE_URL не установлен")
logger.info(f"Исходный DATABASE_URL: {DATABASE_URL}")

# Заменяем схему postgres:// на postgresql+asyncpg://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://")
logger.info(f"Обновлённый DATABASE_URL: {DATABASE_URL}")

# Создаём движок SQLAlchemy с параметрами для asyncpg
try:
    engine = create_async_engine(
        DATABASE_URL,
        echo=True,  # Включаем отладку SQL-запросов
        pool_size=5,
        max_overflow=10,
        connect_args={"server_settings": {"application_name": "stock-market-collector"}}
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