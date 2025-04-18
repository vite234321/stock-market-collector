import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text
from .models import Signal
from datetime import datetime
import httpx
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
async def detect_anomalies(db: AsyncSession):
    query = """
    SELECT ticker, last_price, LAG(last_price) OVER (PARTITION BY ticker ORDER BY updated_at) as prev_price
    FROM stocks
    WHERE updated_at >= NOW() - INTERVAL '10 minutes'
    """
    try:
        result = await db.execute(text(query))
        for row in result:
            ticker, last_price, prev_price = row
            if prev_price and abs(last_price - prev_price) / prev_price > 0.05:
                signal = Signal(
                    ticker=ticker,
                    signal_type="price_spike",
                    value=last_price,
                    created_at=datetime.utcnow()
                )
                db.add(signal)
                async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(retries=3)) as client:
                    await client.post("https://stock-market-bot.herokuapp.com/signals", json={
                        "ticker": ticker,
                        "signal_type": "price_spike",
                        "value": last_price
                    })
        await db.commit()
        logger.info("Anomaly detection completed")
    except Exception as e:
        logger.error(f"Error during anomaly detection: {e}")

async def detect_anomalies_for_ticker(ticker: str, last_price: float, volume: float, db: AsyncSession):
    try:
        query = """
        SELECT last_price
        FROM stocks
        WHERE ticker = :ticker AND updated_at >= NOW() - INTERVAL '10 minutes'
        ORDER BY updated_at DESC
        LIMIT 1 OFFSET 1
        """
        result = await db.execute(text(query).bindparams(ticker=ticker))
        prev_price = result.scalar()
        if prev_price and abs(last_price - prev_price) / prev_price > 0.05:
            return {"type": "price_spike", "value": last_price}
        return None
    except Exception as e:
        logger.error(f"Error detecting anomalies for {ticker}: {e}")
        return None
