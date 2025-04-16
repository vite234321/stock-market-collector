from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from .models import Stock, Signal
from .schemas import StockBase, SignalBase
from .database import get_db
from .scheduler import scheduler

app = FastAPI(title="Data Collector API")

@app.on_event("startup")
async def startup_event():
    scheduler.start()

@app.on_event("shutdown")
async def shutdown_event():
    scheduler.shutdown()

@app.get("/stocks", response_model=list[StockBase])
async def get_stocks(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Stock))
    stocks = result.scalars().all()
    if not stocks:
        raise HTTPException(status_code=404, detail="Акции не найдены")
    return stocks

@app.get("/stocks/{ticker}", response_model=StockBase)
async def get_stock(ticker: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Stock).where(Stock.ticker == ticker))
    stock = result.scalars().first()
    if not stock:
        raise HTTPException(status_code=404, detail=f"Акция {ticker} не найдена")
    return stock

@app.get("/signals", response_model=list[SignalBase])
async def get_signals(ticker: str | None = None, db: AsyncSession = Depends(get_db)):
    query = select(Signal)
    if ticker:
        query = query.where(Signal.ticker == ticker)
    result = await db.execute(query)
    signals = result.scalars().all()
    return signals