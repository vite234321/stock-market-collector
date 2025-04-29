import asyncio
import logging
from datetime import datetime
import httpx
from fastapi import FastAPI, Depends
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import select, update
from aiogram import Bot
from .database import async_session  # Возвращаем относительный импорт
from .models import Stock, Signal, Subscription  # Относительный импорт
from dotenv import load_dotenv
import os

# Загружаем переменные окружения
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация Telegram-бота
logger.info("Инициализация Telegram-бота...")
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не установлен")
bot = Bot(token=BOT_TOKEN)
logger.info("Telegram-бот успешно инициализирован.")

# Список тикеров для обработки
async def fetch_tickers():
    try:
        async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(retries=3)) as client:
            response = await client.get("https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json")
            data = response.json()
            if "securities" not in data or "data" not in data["securities"]:
                raise KeyError("Неверная структура ответа от MOEX")
            columns = data["securities"]["columns"]
            secid_index = columns.index("SECID")  # Проверяем, есть ли SECID в колонках
            tickers = [f"{row[secid_index]}.ME" for row in data["securities"]["data"] if row[secid_index]]
            logger.info(f"Получено {len(tickers)} тикеров: {tickers[:5]}...")
            return tickers
    except Exception as e:
        logger.error(f"Ошибка получения списка тикеров с MOEX: {e}")
        logger.info("Используем резервный список тикеров.")
        return ["SBER.ME", "GAZP.ME", "LKOH.ME", "YNDX.ME", "ROSN.ME"]

# Получение данных через прямой запрос к API MOEX
async def fetch_stock_data_moex(ticker: str, client: httpx.AsyncClient):
    try:
        url = f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/{ticker.replace('.ME', '')}.json"
        response = await client.get(url)
        data = response.json()
        securities_data = data.get('securities', {}).get('data', [])
        marketdata = data.get('marketdata', {}).get('data', [])
        
        stock_name = None
        last_price = None
        volume = 0
        
        # Получаем имя акции из securities
        if securities_data:
            columns = data['securities']['columns']
            shortname_idx = columns.index('SHORTNAME')
            stock_name = securities_data[0][shortname_idx]
        
        # Получаем цену и объём из marketdata
        if marketdata:
            columns = data['marketdata']['columns']
            last_idx = columns.index('LAST')
            volume_idx = columns.index('VOLUME')
            last_price = marketdata[0][last_idx]
            volume = marketdata[0][volume_idx] if volume_idx < len(marketdata[0]) else 0

        if last_price is None:
            logger.warning(f"Не удалось получить цену для {ticker} через прямой запрос")
            return None, None, None
        
        return stock_name or ticker, last_price, volume
    except Exception as e:
        logger.error(f"Ошибка прямого запроса к API MOEX для {ticker}: {e}")
        return None, None, None

# Функция для анализа аномалий
async def detect_anomalies_for_ticker(ticker: str, last_price: float, volume: int, db: 'AsyncSession') -> dict:
    try:
        result = await db.execute(select(Stock).where(Stock.ticker == ticker))
        stock = result.scalars().first()
        if stock and last_price > stock.last_price * 1.05:
            return {"type": "рост цены", "value": last_price}
        return None
    except Exception as e:
        logger.error(f"Ошибка анализа аномалий для {ticker}: {e}")
        return None

async def collect_stock_data(tickers):
    logger.info(f"Начало сбора данных для {len(tickers)} тикеров")
    try:
        async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(retries=3)) as client:
            logger.info("HTTP-клиент успешно инициализирован.")
            async with async_session() as db:
                logger.info("Подключение к базе данных успешно установлено.")
                logger.info("Проверка состояния базы данных: выполнение тестового запроса...")
                test_query = await db.execute(select(Stock))
                test_result = test_query.scalars().all()
                logger.info(f"Тестовый запрос выполнен. Найдено записей в таблице stocks: {len(test_result)}")
                
                for ticker in tickers:
                    logger.info(f"Обработка тикера: {ticker}")
                    for attempt in range(1, 4):
                        try:
                            logger.info(f"Попытка {attempt}: прямой запрос к API MOEX для {ticker}")
                            stock_name, last_price, volume = await fetch_stock_data_moex(ticker, client)
                            if last_price is None:
                                if attempt == 3:
                                    logger.error(f"Не удалось получить данные для {ticker} после 3 попыток.")
                                    break
                                await asyncio.sleep(2)
                                continue

                            logger.info(f"Получены данные для {ticker}: цена={last_price}, объём={volume}")
                            # ... (остальной код для работы с базой данных)
                        except Exception as e:
                            logger.warning(f"Ошибка получения данных для {ticker} на попытке {attempt}: {e}")
                            if attempt == 3:
                                logger.error(f"Не удалось обработать {ticker} после 3 попыток: {e}")
                                break
                            await asyncio.sleep(2)
    except Exception as e:
        logger.error(f"Ошибка инициализации HTTP-клиента: {e}")
    finally:
        logger.info("Сбор данных завершён")

# Планировщик для периодического сбора данных
scheduler = AsyncIOScheduler()
TICKERS = []  # Инициализируем пустой список

# Используем lifespan вместо устаревшего on_event
@asynccontextmanager
async def lifespan(app: FastAPI):
    global TICKERS
    logger.info("Запуск коллектора...")
    logger.info("Получение списка всех тикеров с MOEX...")
    TICKERS = await fetch_tickers()
    logger.info(f"Итоговый список тикеров: {TICKERS[:5]}...")
    logger.info("Запуск немедленного сбора данных...")
    await collect_stock_data(TICKERS)
    scheduler.add_job(collect_stock_data, "interval", minutes=10, args=[TICKERS])
    logger.info("Запуск цикла для периодического сбора данных...")
    scheduler.start()

    yield

    logger.info("Завершение работы коллектора...")
    scheduler.shutdown()

# Передаём lifespan в FastAPI
app = FastAPI(lifespan=lifespan)

@app.get("/health")
async def health_check():
    return {"status": "healthy"}