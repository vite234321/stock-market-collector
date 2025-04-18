import logging
import asyncio
import yfinance as yf
from sqlalchemy.ext.asyncio import AsyncSession
from .models import Stock
from datetime import datetime
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
async def collect_stock_data(ticker: str, db: AsyncSession):
    for attempt in range(1, 4):
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
                logger.info(f"Collected data for {ticker}")
                return
            else:
                
                logger.warning(f"No data for {ticker} on attempt {attempt}")
                if attempt == 3:
                    return
        except Exception as e:
            logger.error(f"Error collecting data for {ticker} on attempt {attempt}: {e}")
            if attempt == 3:
                return
        await asyncio.sleep(1)
