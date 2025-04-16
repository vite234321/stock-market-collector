from apscheduler.schedulers.asyncio import AsyncIOScheduler
from .collector import collect_stock_data
from .anomaly_detector import detect_anomalies
from .database import get_db

scheduler = AsyncIOScheduler()

async def schedule_tasks():
    async with get_db() as db:
        tickers = ["SBER.ME", "GAZP.ME", "LKOH.ME"]
        for ticker in tickers:
            await collect_stock_data(ticker, db)
        await detect_anomalies(db)

scheduler.add_job(schedule_tasks, "interval", minutes=5)