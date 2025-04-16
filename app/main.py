from fastapi import FastAPI, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select, update
from .database import get_db
from .models import Stock, Signal
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from .anomaly_detector import detect_anomalies
import yfinance as yf
import httpx

app = FastAPI()
scheduler = AsyncIOScheduler()

# Список тикеров (заменили на американские акции)
TICKERS = ["AAPL", "GOOGL", "TSLA"]

@app.on_event("startup")
async def startup_event():
    scheduler.start()
    scheduler.add_job(collect_stock_data, "interval", minutes=5)

@app.on_event("shutdown")
async def shutdown_event():
    scheduler.shutdown()

async def collect_stock_data():
    async with httpx.AsyncClient() as client:
        for ticker in TICKERS:
            try:
                stock = yf.Ticker(ticker)
                data = stock.history(period="5m")
                if data.empty:
                    print(f"Нет данных для {ticker}")
                    continue
                
                last_price = data["Close"][-1]
                volume = data["Volume"][-1]
                
                async with app.state.db() as db:
                    # Обновляем или создаём запись
                    result = await db.execute(select(Stock).where(Stock.ticker == ticker))
                    stock_entry = result.scalars().first()
                    if stock_entry:
                        await db.execute(
                            update(Stock).where(Stock.ticker == ticker),
                            {"last_price": last_price, "volume": volume}
                        )
                    else:
                        new_stock = Stock(ticker=ticker, name=ticker, last_price=last_price, volume=volume)
                        db.add(new_stock)
                    await db.commit()
                    
                    # Анализ аномалий
                    try:
                        signal = detect_anomalies(ticker, last_price, volume)
                        if signal:
                            new_signal = Signal(ticker=ticker, signal_type=signal["type"], value=signal["value"])
                            db.add(new_signal)
                            await db.commit()
                            # Отправка сигнала в telegram-bot
                            await client.post("https://stock-market-bot.herokuapp.com/signals", json={
                                "ticker": ticker,
                                "signal_type": signal["type"],
                                "value": signal["value"]
                            })
                    except Exception as e:
                        print(f"Ошибка при анализе аномалий: {e}")
            except Exception as e:
                print(f"Ошибка при получении данных для {ticker}: {e}")

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
