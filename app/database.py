import logging
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL").replace("postgres://", "postgresql+asyncpg://")

logger.info("Создание движка SQLAlchemy для коллектора с отключением кэша prepared statements...")
engine = create_async_engine(
    DATABASE_URL,
    echo=True,
    connect_args={"statement_cache_size": 0}  # Отключаем кэш prepared statements
)
logger.info("Движок SQLAlchemy для коллектора создан успешно.")

async_session = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False
)

class Base(DeclarativeBase):
    pass

async def get_db() -> AsyncSession:
    logger.info("Открытие сессии базы данных...")
    async with async_session() as session:
        logger.info("Сессия базы данных открыта.")
        yield session
    logger.info("Сессия базы данных закрыта.")