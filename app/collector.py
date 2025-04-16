import yfinance as yf
from sqlalchemy.ext.asyncio import AsyncSession
from .models import Stock
from datetime import datetime
import asyncio

async def collect_stock_data(ticker: str, db: AsyncSession):
    try:
        stock = yf.Ticker(ticker)
        data = stock.history(period="5m")
        if not data.empty:
            last_price = data["Close"].iloc[-1]
            volume = data["Volume"].iloc[-1]
            stock_entry = Stock(
                ticker=ticker,
                name=stock.info.get("shortName", ticker),
                last_price=last_price,
                volume=volume,
                updated_at=datetime.utcnow()
            )
            db.add(stock_entry)
            await db.commit()
        else:
            print(f"Нет данных для {ticker}")
    except Exception as e:
        print(f"Ошибка при сборе данных для {ticker}: {e}")
    await asyncio.sleep(1)