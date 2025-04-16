from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase
from datetime import datetime

class Base(AsyncAttrs, DeclarativeBase):
    pass

class Stock(Base):
    tablename = "stocks"
    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String, unique=True, index=True)
    name = Column(String)
    last_price = Column(Float)
    volume = Column(Float)
    updated_at = Column(DateTime, default=datetime.utcnow)

class Signal(Base):
    tablename = "signals"
    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String, index=True)
    signal_type = Column(String)
    value = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)