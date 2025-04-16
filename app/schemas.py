from pydantic import BaseModel
from datetime import datetime

class StockBase(BaseModel):
    ticker: str
    name: str
    last_price: float
    volume: float
    updated_at: datetime

    class Config:
        from_attributes = True

class SignalBase(BaseModel):
    ticker: str
    signal_type: str
    value: float
    created_at: datetime

    class Config:
        from_attributes = True