import logging
import asyncio
from moexalgo import Market, Ticker
from sqlalchemy.ext.asyncio import AsyncSession
from .models import Stock
from datetime import datetime
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
async def collect_stock_data(ticker: str, db: AsyncSession):
    for attempt in range(1, 4):
        try:
            stock = Ticker(ticker, market=Market('stocks'))
            data = stock.price_info()
            if not data or 'LAST' not in data:
                logger.warning(f"No data for {ticker} on attempt {attempt}")
                if attempt == 3:
                    return
                await asyncio.sleep(1)
                continue
            last_price = data['LAST']
            volume = data.get('VOLUME', 0)
            stock_entry = Stock(
                ticker=ticker,
                name=stock.info.get('shortName', ticker),
                last_price=last_price,
                volume=volume,
                updated_at=datetime.utcnow()
            )
            db.add(stock_entry)
            await db.commit()
            logger.info(f"Collected data for {ticker}")
            return
        except Exception as e:
            logger.error(f"Error collecting data for {ticker} on attempt {attempt}: {e}")
            if attempt == 3:
                return
            await asyncio.sleep(1)
