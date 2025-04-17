from fastapi import FastAPI, Depends, HTTPException
import asyncio
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import select, update
from .database import get_db
from .models import Stock, Signal
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from .anomaly_detector import detect_anomalies_for_ticker
from datetime import datetime
import yfinance as yf
import httpx

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()
scheduler = AsyncIOScheduler()

# Список тикеров (используем американские акции)
TICKERS = ["AAPL", "GOOGL", "TSLA"]

@app.on_event("startup")
async def startup_event():
    scheduler.start()
    scheduler.add_job(collect_stock_data, "interval", minutes=5)

@app.on_event("shutdown")
async def shutdown_event():
    scheduler.shutdown()

async def collect_stock_data():
    async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(retries=3)) as client:
        for ticker in TICKERS:
            for attempt in range(1, 4):
                try:
                    stock = yf.Ticker(ticker)
                    data = stock.history(period="5m")
                    if data.empty:
                        logger.warning(f"No data for {ticker} on attempt {attempt}")
                        if attempt == 3:
                            continue
                        await asyncio.sleep(2)
                        continue
                    
                    last_price = data["Close"][-1]
                    volume = data["Volume"][-1]
                    
                    async with get_db() as db:
                        # Обновляем или создаём запись
                        result = await db.execute(select(Stock).where(Stock.ticker == ticker))
                        stock_entry = result.scalars().first()
                        if stock_entry:
                            await db.execute(
                                update(Stock).where(Stock.ticker == ticker).values(
                                    last_price=last_price, volume=volume, updated_at=datetime.utcnow()
                                )
                            )
                        else:
                            new_stock = Stock(
                                ticker=ticker, name=ticker, last_price=last_price, volume=volume
                            )
                            db.add(new_stock)
                        await db.commit()
                        
                        # Анализ аномалий
                        try:
                            signal = await detect_anomalies_for_ticker(ticker, last_price, volume, db)
                            if signal:
                                new_signal = Signal(
                                    ticker=ticker, signal_type=signal["type"], value=signal["value"]
                                )
                                db.add(new_signal)
                                await db.commit()
                                # Отправка сигнала в telegram-bot
                                await client.post("https://stock-market-bot.herokuapp.com/signals", json={
                                    "ticker": ticker,
                                    "signal_type": signal["type"],
                                    "value": signal["value"]
                                })
                        except Exception as e:
                            logger.error(f"Error analyzing anomalies for {ticker}: {e}")
                    break
                except Exception as e:
                    logger.warning(f"Error fetching data for {ticker} on attempt {attempt}: {e}")
                    if attempt == 3:
                        break
                    await asyncio.sleep(2)

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
