import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from .collector import collect_stock_data
from .anomaly_detector import detect_anomalies
from .database import get_db

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()

async def schedule_tasks():
    try:
        async with get_db() as db:
            tickers = ["SBER.ME", "GAZP.ME", "LKOH.ME"]
            for ticker in tickers:
                await collect_stock_data(ticker, db)
            await detect_anomalies(db)
        logger.info("Scheduled tasks completed")
    except Exception as e:
        logger.error(f"Error in scheduled tasks: {e}")

scheduler.add_job(schedule_tasks, "interval", minutes=5)
