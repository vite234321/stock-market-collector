# app/database.py
import os
import asyncio
import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# Получение строки подключения из переменной окружения
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL не установлен в переменных окружения")

# Преобразуем postgres:// или postgresql+asyncpg:// на postgresql+psycopg://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg://", 1)
elif DATABASE_URL.startswith("postgresql+asyncpg://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql+psycopg://", 1)

logger.info(f"Строка подключения к базе данных: {DATABASE_URL}")

# Создание асинхронного движка
try:
    engine = create_async_engine(
        DATABASE_URL,
        echo=True,
        pool_pre_ping=True,  # Проверяем соединения перед использованием
        connect_args={"timeout": 30}  # Увеличиваем тайм-аут до 30 секунд
    )
    logger.info("Движок SQLAlchemy для коннектора создан успешно")
except Exception as e:
    logger.error(f"Ошибка создания движка SQLAlchemy: {e}")
    raise

# Создание фабрики сессий
async_session = sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False
)
logger.info("Фабрика сессий SQLAlchemy создана успешно")

# Базовый класс для моделей SQLAlchemy
Base = declarative_base()

# Функция для инициализации базы данных (создание таблиц) с повторными попытками
async def init_db():
    for attempt in range(1, 10):  # 10 попыток
        try:
            logger.info(f"Попытка {attempt}: подключение к базе данных...")
            async with engine.begin() as conn:
                logger.info("Соединение с базой данных успешно установлено.")
                await conn.run_sync(Base.metadata.create_all)
                logger.info("Таблицы успешно созданы или уже существуют.")
                return
        except Exception as e:
            logger.error(f"Ошибка подключения к базе данных на попытке {attempt}: {e}")
            if attempt == 9:
                logger.error("Не удалось подключиться к базе данных после 10 попыток. Завершаем работу.")
                raise
            await asyncio.sleep(10)  # Задержка 10 секунд перед следующей попыткой