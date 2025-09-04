# models.py
from pydantic import BaseModel
from typing import Optional

class Signal(BaseModel):
    action: str
    symbol: str
    price: float
    stop_loss: Optional[float] = None
    take_profit_1: Optional[float] = None
    take_profit_2: Optional[float] = None
    take_profit_3: Optional[float] = None

class Trade(BaseModel):
    trade_id: Optional[int] = None
    user_id: int
    exchange: str
    order_id: str
    symbol: str
    side: str
    position_side: str
    quantity: float
    entry_price: float
    stop_loss: Optional[float] = None
    take_profit_1: Optional[float] = None
    take_profit_2: Optional[float] = None
    take_profit_3: Optional[float] = None
    sl_order_id: Optional[str] = None
    tp1_order_id: Optional[str] = None
    tp2_order_id: Optional[str] = None
    tp3_order_id: Optional[str] = None
    status: str

class User(BaseModel):
    user_id: int
    api_key: str
    secret_key: str
    passphrase: Optional[str] = None
    exchange: str