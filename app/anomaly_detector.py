from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text
from .models import Signal
from datetime import datetime
import httpx

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
                async with httpx.AsyncClient() as client:
                    await client.post("http://telegram-bot:8001/signals", json={
                        "ticker": ticker,
                        "signal_type": "price_spike",
                        "value": last_price
                    })
        await db.commit()
    except Exception as e:
        print(f"Ошибка при анализе аномалий: {e}")