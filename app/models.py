from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, BigInteger
from sqlalchemy.orm import relationship
from .database import Base
from datetime import datetime

class Stock(Base):
    __tablename__ = "stocks"

    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String(50), unique=True, index=True, nullable=False)
    name = Column(String(255))
    last_price = Column(Float)
    volume = Column(Float)  # Изменили на Float, чтобы соответствовать базе данных
    updated_at = Column(DateTime, default=datetime.utcnow)

    signals = relationship("Signal", back_populates="stock")
    subscriptions = relationship("Subscription", back_populates="stock")

class Signal(Base):
    __tablename__ = "signals"

    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String(50), ForeignKey("stocks.ticker"), nullable=False)
    signal_type = Column(String(50), nullable=False)
    value = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    stock = relationship("Stock", back_populates="signals")

class Subscription(Base):
    __tablename__ = "subscriptions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(BigInteger, nullable=False)
    ticker = Column(String(50), ForeignKey("stocks.ticker"), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    stock = relationship("Stock", back_populates="subscriptions")