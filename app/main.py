from fastapi import FastAPI, Depends, HTTPException
import asyncio
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import select, update
from .database import get_db
from .models import Stock, Signal, Subscription
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from .anomaly_detector import detect_anomalies_for_ticker
from datetime import datetime
from moexalgo import Market, Ticker
import httpx
from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()
scheduler = AsyncIOScheduler()

# Инициализация бота
bot = Bot(
    token=os.getenv("BOT_TOKEN"),
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

# Расширенный список тикеров
TICKERS = [
    "SBER.ME", "GAZP.ME", "LKOH.ME", "YNDX.ME", "ROSN.ME",
    "TATN.ME", "VTBR.ME", "MGNT.ME", "NVTK.ME", "GMKN.ME"
]

@app.on_event("startup")
async def startup_event():
    logger.info("Запуск коллектора...")
    scheduler.start()
    scheduler.add_job(collect_stock_data, "interval", minutes=5)

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Завершение работы коллектора...")
    scheduler.shutdown()
    await bot.session.close()

async def collect_stock_data():
    logger.info("Начало сбора данных для тикеров...")
    async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(retries=3)) as client:
        async with get_db() as db:
            for ticker in TICKERS:
                logger.info(f"Обработка тикера: {ticker}")
                for attempt in range(1, 4):
                    try:
                        stock = Ticker(ticker.replace(".ME", ""), market=Market('stocks'))
                        data = stock.price_info()
                        if not data or 'LAST' not in data:
                            logger.warning(f"Нет данных для {ticker} на попытке {attempt}")
                            if attempt == 3:
                                continue
                            await asyncio.sleep(2)
                            continue

                        last_price = data['LAST']
                        volume = data.get('VOLRUR', 0)
                        logger.info(f"Получены данные для {ticker}: цена={last_price}, объём={volume}")

                        # Обновляем или создаём запись
                        result = await db.execute(select(Stock).where(Stock.ticker == ticker))
                        stock_entry = result.scalars().first()
                        if stock_entry:
                            await db.execute(
                                update(Stock).where(Stock.ticker == ticker).values(
                                    last_price=last_price, volume=volume, updated_at=datetime.utcnow()
                                )
                            )
                            logger.info(f"Обновлена запись для {ticker}")
                        else:
                            new_stock = Stock(
                                ticker=ticker,
                                name=stock.info.get('shortName', ticker),
                                last_price=last_price,
                                volume=volume
                            )
                            db.add(new_stock)
                            logger.info(f"Создана новая запись для {ticker}")
                        await db.commit()

                        # Анализ аномалий
                        try:
                            signal = await detect_anomalies_for_ticker(ticker, last_price, volume, db)
                            if signal:
                                new_signal = Signal(
                                    ticker=ticker,
                                    signal_type=signal["type"],
                                    value=signal["value"]
                                )
                                db.add(new_signal)
                                await db.commit()
                                logger.info(f"Сохранён сигнал для {ticker}: {signal}")

                                # Отправка сигнала в telegram-bot
                                await client.post("https://stock-market-bot.herokuapp.com/signals", json={
                                    "ticker": ticker,
                                    "signal_type": signal["type"],
                                    "value": signal["value"]
                                })
                                logger.info(f"Сигнал отправлен в stock-market-bot для {ticker}")

                                # Отправка уведомлений подписчикам
                                subscriptions = await db.execute(
                                    select(Subscription).where(Subscription.ticker == ticker)
                                )
                                subscriptions = subscriptions.scalars().all()
                                logger.info(f"Найдено {len(subscriptions)} подписчиков для {ticker}")
                                for sub in subscriptions:
                                    try:
                                        await bot.send_message(
                                            chat_id=sub.user_id,
                                            text=f"📈 Акция <b>{ticker}</b> выросла на более чем 5%! Текущая цена: {signal['value']} RUB"
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
                            break
                        await asyncio.sleep(2)
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