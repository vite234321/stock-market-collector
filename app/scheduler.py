import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from .main import collect_stock_data
from .anomaly_detector import detect_anomalies

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()

async def schedule_tasks():
    try:
        await collect_stock_data()
        async with get_db() as db:
            await detect_anomalies(db)
        logger.info("Scheduled tasks completed")
    except Exception as e:
        logger.error(f"Error in scheduled tasks: {e}")

scheduler.add_job(schedule_tasks, "interval", minutes=5)