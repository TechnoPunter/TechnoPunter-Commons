from sqlalchemy import Column, Integer, String, Numeric

from commons.dataprovider.database import Base


class TradeLog(Base):
    __tablename__ = 'trade_log'

    trade_log_id = Column(Integer, primary_key=True)
    acct = Column(String)
    trade_date = Column(String)
    trade_type = Column(String)
    scrip = Column(String)
    model = Column(String)
    signal = Column(Integer)
    target = Column(Numeric)
    quantity = Column(Integer)
    entry_price = Column(Numeric)
    entry_time = Column(Integer)
    status = Column(String)
    exit_price = Column(Numeric)
    exit_time = Column(Integer)
    pnl = Column(Numeric)
    sl = Column(Numeric)
    sl_update_cnt = Column(Numeric)
    max_mtm = Column(Numeric)
    max_mtm_pct = Column(Numeric)
