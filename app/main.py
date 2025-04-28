import asyncio
import logging
from datetime import datetime
import httpx
from fastapi import FastAPI, Depends
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from moexalgo import Ticker
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

# Основная функция для сбора данных
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
                            stock = Ticker(ticker.replace(".ME", ""), session='TQBR')
                            logger.info(f"Объект Ticker для {ticker} создан.")
                            
                            logger.info(f"Попытка {attempt}: получение информации об акции {ticker}")
                            stock_info = stock.info()
                            logger.info(f"Информация об акции {ticker}: {stock_info}")
                            stock_name = stock_info.get('SHORTNAME', ticker) if isinstance(stock_info, dict) else getattr(stock_info, 'SHORTNAME', ticker)
                            logger.info(f"Имя акции для {ticker}: {stock_name}")

                            logger.info(f"Попытка {attempt}: получение ценовых данных для {ticker}")
                            price_data = stock.price_info()
                            logger.info(f"Ценовые данные для {ticker}: {price_data}")
                            if not price_data or 'LAST' not in price_data:
                                logger.warning(f"Нет ценовых данных для {ticker} на попытке {attempt}")
                                if attempt == 3:
                                    logger.error(f"Не удалось получить ценовые данные для {ticker} после 3 попыток.")
                                    continue
                                await asyncio.sleep(2)
                                continue

                            last_price = price_data['LAST']
                            volume = price_data.get('VOLUME', 0)
                            logger.info(f"Получены данные для {ticker}: цена={last_price}, объём={volume}")

                            logger.info(f"Поиск записи для {ticker} в базе данных...")
                            result = await db.execute(select(Stock).where(Stock.ticker == ticker))
                            stock_entry = result.scalars().first()
                            logger.info(f"Результат поиска: {stock_entry}")
                            if stock_entry:
                                logger.info(f"Запись для {ticker} найдена, обновляем...")
                                update_query = update(Stock).where(Stock.ticker == ticker).values(
                                    last_price=last_price,
                                    volume=volume,
                                    updated_at=datetime.utcnow()
                                )
                                logger.info(f"Выполнение запроса на обновление: {update_query}")
                                await db.execute(update_query)
                                logger.info(f"Запись для {ticker} обновлена: цена={last_price}, объём={volume}")
                            else:
                                logger.info(f"Запись для {ticker} не найдена, создаём новую...")
                                new_stock = Stock(
                                    ticker=ticker,
                                    name=stock_name,
                                    last_price=last_price,
                                    volume=volume
                                )
                                logger.info(f"Добавление новой записи: {new_stock.__dict__}")
                                db.add(new_stock)
                                logger.info(f"Новая запись для {ticker} создана: цена={last_price}, объём={volume}")
                            logger.info(f"Сохранение изменений для {ticker} в базе данных...")
                            await db.commit()
                            logger.info(f"Коммит изменений для {ticker} выполнен.")

                            # Анализ аномалий
                            try:
                                logger.info(f"Запуск анализа аномалий для {ticker}...")
                                signal = await detect_anomalies_for_ticker(ticker, last_price, volume, db)
                                if signal:
                                    new_signal = Signal(
                                        ticker=ticker,
                                        signal_type=signal["type"],
                                        value=signal["value"],
                                        created_at=datetime.utcnow()
                                    )
                                    db.add(new_signal)
                                    await db.commit()
                                    logger.info(f"Сохранён сигнал для {ticker}: {signal}")

                                    logger.info(f"Отправка сигнала для {ticker} в stock-market-bot...")
                                    await client.post("https://stock-market-bot.herokuapp.com/signals", json={
                                        "ticker": ticker,
                                        "signal_type": signal["type"],
                                        "value": signal["value"]
                                    })
                                    logger.info(f"Сигнал отправлен в stock-market-bot для {ticker}")

                                    logger.info(f"Поиск подписчиков для {ticker}...")
                                    subscriptions = await db.execute(
                                        select(Subscription).where(Subscription.ticker == ticker)
                                    )
                                    subscriptions = subscriptions.scalars().all()
                                    logger.info(f"Найдено {len(subscriptions)} подписчиков для {ticker}")
                                    for sub in subscriptions:
                                        try:
                                            await bot.send_message(
                                                chat_id=sub.user_id,
                                                text=f"📈 Акция <b>{ticker}</b> ({stock_name}): {signal['type']}! Текущая цена: {signal['value']} RUB"
                                            )
                                            logger.info(f"Уведомление отправлено пользователю {sub.user_id}")
                                        except Exception as e:
                                            logger.error(f"Ошибка отправки уведомления пользователю {sub.user_id}: {e}")
                            except Exception as e:
                                logger.error(f"Ошибка анализа аномалий для {ticker}: {e}")
                            break
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