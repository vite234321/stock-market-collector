# app/main.py
import asyncio
import logging
import os
from datetime import datetime

import httpx
from aiogram import Bot
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import select, update

from .database import async_session, init_db
from .models import Stock, Signal, Subscription

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# Инициализация Telegram-бота
BOT_TOKEN = os.getenv("BOT_TOKEN")  # Изменили TELEGRAM_TOKEN на BOT_TOKEN
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не установлен в переменных окружения")
bot = Bot(token=BOT_TOKEN)

# Функция для получения тикеров с MOEX
async def fetch_tickers():
    try:
        async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(retries=3)) as client:
            response = await client.get("https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json")
            response.raise_for_status()
            data = response.json()
            if "securities" not in data or "data" not in data["securities"]:
                raise KeyError("Неверная структура ответа от MOEX")
            columns = data["securities"]["columns"]
            secid_col = "SECID" if "SECID" in columns else "secid"
            if secid_col not in columns:
                raise KeyError("Колонка SECID/secid отсутствует в данных MOEX")
            secid_index = columns.index(secid_col)
            tickers = [f"{row[secid_index]}.ME" for row in data["securities"]["data"] if row[secid_index]]
            logger.info(f"Получено {len(tickers)} тикеров: {tickers[:5]}...")
            return tickers
    except Exception as e:
        logger.error(f"Ошибка получения списка тикеров с MOEX: {e}")
        logger.info("Используем резервный список тикеров.")
        return ["SBER.ME", "GAZP.ME", "LKOH.ME", "YNDX.ME", "ROSN.ME"]

# Функция для получения данных о тикере с MOEX
async def fetch_stock_data_moex(ticker, client):
    ticker_moex = ticker.replace(".ME", "")
    url = f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/{ticker_moex}.json"
    try:
        response = await client.get(url)
        response.raise_for_status()
        data = response.json()
        if "marketdata" not in data or not data["marketdata"]["data"]:
            return ticker_moex, None, None
        market_data = data["marketdata"]["data"][0]
        columns = data["marketdata"]["columns"]
        last_price = market_data[columns.index("LAST")] if "LAST" in columns else None
        volume = market_data[columns.index("VOLUME")] if "VOLUME" in columns else None
        return ticker_moex, last_price, volume
    except Exception as e:
        logger.error(f"Ошибка MOEX для {ticker}: {e}")
        return ticker_moex, None, None

# Функция для анализа аномалий (заглушка, можно реализовать позже)
async def detect_anomalies_for_ticker(ticker, last_price, volume, db):
    return None  # Реализуйте логику анализа аномалий, если нужно

# Функция для сбора данных
async def collect_stock_data(tickers):
    logger.info(f"Начало сбора данных для {len(tickers)} тикеров")
    for retry in range(5):
        try:
            async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(retries=3)) as client:
                logger.info("HTTP-клиент успешно инициализирован.")
                db = async_session()
                try:
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

                                # Работа с базой данных
                                logger.info(f"Поиск записи для {ticker} в базе данных...")
                                result = await db.execute(select(Stock).where(Stock.ticker == ticker))
                                stock_entry = result.scalars().first()
                                logger.info(f"Результат поиска: {stock_entry}")
                                if stock_entry:
                                    logger.info(f"Запись для {ticker} найдена, обновляем...")
                                    await db.execute(
                                        update(Stock).where(Stock.ticker == ticker).values(
                                            last_price=last_price,
                                            volume=volume,
                                            updated_at=datetime.utcnow()
                                        )
                                    )
                                    logger.info(f"Запись для {ticker} обновлена: цена={last_price}, объём={volume}")
                                else:
                                    logger.info(f"Запись для {ticker} не найдена, создаём новую...")
                                    new_stock = Stock(
                                        ticker=ticker,
                                        name=stock_name,
                                        last_price=last_price,
                                        volume=volume,
                                        updated_at=datetime.utcnow()
                                    )
                                    logger.info(f"Добавление новой записи: {new_stock.__dict__}")
                                    db.add(new_stock)
                                    logger.info(f"Новая запись для {ticker} создана: цена={last_price}, объём={volume}")
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
                                        response = await client.post("https://stock-market-bot.herokuapp.com/signals", json={
                                            "ticker": ticker,
                                            "signal_type": signal["type"],
                                            "value": signal["value"]
                                        })
                                        logger.info(f"Сигнал отправлен в stock-market-bot для {ticker}: {response.status_code}")
                                        response.raise_for_status()

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
                                    await db.rollback()
                                break
                            except Exception as e:
                                logger.warning(f"Ошибка получения данных для {ticker} на попытке {attempt}: {e}")
                                if attempt == 3:
                                    logger.error(f"Не удалось обработать {ticker} после 3 попыток: {e}")
                                    await db.rollback()
                                    break
                                await asyncio.sleep(2)
                                await db.rollback()
                    break
                finally:
                    await db.close()
        except Exception as e:
            logger.error(f"Ошибка инициализации HTTP-клиента (попытка {retry + 1}): {e}")
            if retry == 4:
                logger.error("Не удалось инициализировать HTTP-клиент после 5 попыток. Прекращаем сбор данных.")
                break
            await asyncio.sleep(5)
        finally:
            logger.info("Сбор данных завершён")

# Основная функция для запуска приложения
async def main():
    logger.info("Инициализация Telegram-бота...")
    global bot
    bot = Bot(token=BOT_TOKEN)  # Изменили TELEGRAM_TOKEN на BOT_TOKEN
    logger.info("Telegram-бот успешно инициализирован.")

    # Инициализация базы данных
    logger.info("Инициализация базы данных...")
    await init_db()
    logger.info("База данных успешно инициализирована.")

    logger.info("Запуск коллектора...")
    tickers = await fetch_tickers()
    logger.info(f"Итоговый список тикеров: {tickers[:5]}...")

    # Немедленный сбор данных при старте
    logger.info("Запуск немедленного сбора данных...")
    await collect_stock_data(tickers)

    # Настройка планировщика для периодического сбора данных
    scheduler = AsyncIOScheduler()
    scheduler.add_job(collect_stock_data, 'interval', minutes=10, args=[tickers])
    logger.info("Запуск цикла для периодического сбора данных...")
    scheduler.start()

    # Бесконечный цикл, чтобы процесс не завершался
    try:
        while True:
            await asyncio.sleep(3600)  # Спим 1 час, чтобы процесс не завершался
    except (KeyboardInterrupt, SystemExit):
        logger.info("Завершение работы коллектора...")
        scheduler.shutdown()
        await bot.session.close()

# Точка входа
if __name__ == "__main__":
    asyncio.run(main())