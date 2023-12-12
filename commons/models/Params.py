from sqlalchemy import Column, Integer, String, Numeric

from commons.dataprovider.database import Base


class Params(Base):
    __tablename__ = 'params_hist'

    params_id = Column(Integer, primary_key=True)
    acct = Column(String)
    trade_date = Column(String)
    close = Column(Numeric)
    signal = Column(Integer)
    target = Column(Numeric)
    scrip = Column(String)
    model = Column(String)
    exchange = Column(String)
    symbol = Column(String)
    token = Column(String)
    target_pct = Column(Numeric)
    sl_pct = Column(Numeric)
    trail_sl_pct = Column(Numeric)
    tick = Column(Numeric)
    type = Column(String)
    risk = Column(Numeric)
    quantity = Column(Integer)
    entry_order_id = Column(String)
    sl_order_id = Column(String)
    target_order_id = Column(String)
    entry_order_status = Column(String)
    sl_order_status = Column(String)
    target_order_status = Column(String)
    entry_ts = Column(Integer)
    sl_ts = Column(Integer)
    target_ts = Column(Integer)
    entry_price = Column(Numeric)
    sl_price = Column(Numeric)
    target_price = Column(Numeric)
    strength = Column(Numeric)
    active = Column(String)
    bod_sl = Column(Numeric)
