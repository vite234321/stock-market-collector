# app/models.py
from sqlalchemy import Column, Integer, String, Float, DateTime
from .database import Base

class Stock(Base):
    __tablename__ = "stocks"
    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String, unique=True, index=True)
    name = Column(String)
    last_price = Column(Float)
    volume = Column(Float)  # Изменяем с Integer на Float
    updated_at = Column(DateTime)

class Signal(Base):
    __tablename__ = "signals"
    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String)
    signal_type = Column(String)
    value = Column(Float)
    created_at = Column(DateTime)

class Subscription(Base):
    __tablename__ = "subscriptions"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer)
    ticker = Column(String)
    created_at = Column(DateTime)