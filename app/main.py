import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("Файл app/main.py импортирован. Начало выполнения.")

from fastapi import FastAPI, Depends, HTTPException
import asyncio
import os
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import select, update
from .database import get_db
from .models import Stock, Signal, Subscription
from .anomaly_detector import detect_anomalies_for_ticker
from datetime import datetime
from moexalgo import Market, Ticker
import httpx
from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from .database import async_session

logger.info("Импорт всех зависимостей завершён.")

app = FastAPI()

logger.info("Инициализация Telegram-бота...")
bot = Bot(
    token=os.getenv("BOT_TOKEN"),
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
logger.info("Telegram-бот успешно инициализирован.")

# Получаем список всех тикеров с MOEX
logger.info("Получение списка всех тикеров с MOEX...")
try:
    market = Market('stocks')
    tickers_df = market.tickers()
    TICKERS = [f"{ticker}.ME" for ticker in tickers_df['SECID'].tolist()]
    logger.info(f"Получено {len(TICKERS)} тикеров: {TICKERS[:5]}...")  # Первые 5 для примера
except Exception as e:
    logger.error(f"Ошибка получения списка тикеров с MOEX: {e}")
    TICKERS = ["SBER.ME", "GAZP.ME", "LKOH.ME", "YNDX.ME", "ROSN.ME"]
    logger.info("Используем резервный список тикеров.")

logger.info(f"Итоговый список тикеров: {TICKERS[:5]}...")

@app.on_event("startup")
async def startup_event():
    logger.info("Запуск коллектора...")
    logger.info("Запуск немедленного сбора данных...")
    try:
        await collect_stock_data()
    except Exception as e:
        logger.error(f"Ошибка при выполнении collect_stock_data на старте: {e}")
    logger.info("Запуск цикла для периодического сбора данных...")
    asyncio.create_task(run_collector())

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Завершение работы коллектора...")
    await bot.session.close()

async def run_collector():
    while True:
        logger.info("Начало циклического сбора данных...")
        try:
            await collect_stock_data()
        except Exception as e:
            logger.error(f"Ошибка при выполнении collect_stock_data в цикле: {e}")
        logger.info("Ожидание 10 минут перед следующим сбором данных...")
        await asyncio.sleep(600)  # 10 минут

# Обновим функцию collect_stock_data
async def collect_stock_data():
    logger.info(f"Начало сбора данных для {len(TICKERS)} тикеров")
    try:
        async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(retries=3)) as client:
            logger.info("HTTP-клиент успешно инициализирован.")
            # Используем async_session напрямую
            async with async_session() as db:
                logger.info("Подключение к базе данных успешно установлено.")
                logger.info("Проверка состояния базы данных: выполнение тестового запроса...")
                test_query = await db.execute(select(Stock))
                test_result = test_query.scalars().all()
                logger.info(f"Тестовый запрос выполнен. Найдено записей в таблице stocks: {len(test_result)}")
                
                for ticker in TICKERS:
                    logger.info(f"Обработка тикера: {ticker}")
                    for attempt in range(1, 4):
                        try:
                            stock = Ticker(ticker.replace(".ME", ""), market=Market('stocks'))
                            logger.info(f"Объект Ticker для {ticker} создан.")
                            
                            logger.info(f"Попытка {attempt}: получение информации об акции {ticker}")
                            stock_info = stock.info
                            logger.info(f"Информация об акции {ticker}: {stock_info}")
                            stock_name = stock_info.get('SHORTNAME', ticker) if isinstance(stock_info, dict) else getattr(stock_info, 'shortName', ticker)
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

@app.get("/stocks")
async def get_stocks(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Stock))
    return result.scalars().all()

@app.get("/stocks/{ticker}")
async def get_stock(ticker: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Stock).where(Stock.ticker == ticker))
    stock = result.scalars().first()
    if not stock:
        raise HTTPException(status_code=404, detail="Stock not found")
    return stock

@app.get("/signals")
async def get_signals(ticker: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Signal).where(Signal.ticker == ticker))
    signals = result.scalars().all()
    if not signals:
        raise HTTPException(status_code=404, detail="No signals found")
    return signals

@app.get("/health")
async def health_check():
    return {"status": "Collector is running"}