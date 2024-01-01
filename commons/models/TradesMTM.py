from sqlalchemy import Column, Integer, String, Numeric

from commons.dataprovider.database import Base


class TradesMTM(Base):
    __tablename__ = 'trades_mtm'

    trade_mtm_id = Column(Integer, primary_key=True)
    acct = Column(String)
    scrip = Column(String)
    strategy = Column(String)
    trade_date = Column(String)
    trade_time = Column(String)
    datetime = Column(String)
    signal = Column(Integer)
    time = Column(Integer)
    open = Column(Numeric)
    high = Column(Numeric)
    low = Column(Numeric)
    close = Column(Numeric)
    target = Column(Numeric)
    target_met = Column(String)
    entry_price = Column(Numeric)
    mtm = Column(Numeric)
    mtm_pct = Column(Numeric)
