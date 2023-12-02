from sqlalchemy import Column, Integer, String, Numeric

from commons.dataprovider.database import Base


# MTM_DF_COLS = [
#     'scrip', 'strategy', 'date', 'datetime', 'signal', 'time', 'open', 'high', 'low', 'close',
#     'target', 'target_met', 'day_close', 'entry_price', 'mtm', 'mtm_pct'
# ]
class TradesMTM(Base):
    __tablename__ = 'trades_mtm'

    trade_mtm_id = Column(Integer, primary_key=True)
    scrip = Column(String)
    strategy = Column(String)
    date = Column(String)
    datetime = Column(String)
    signal = Column(Integer)
    time = Column(Integer)
    open = Column(Numeric)
    high = Column(Numeric)
    low = Column(Numeric)
    close = Column(Numeric)
    target = Column(Numeric)
    target_met = Column(String)
    day_close = Column(Numeric)
    entry_price = Column(Numeric)
    mtm = Column(Numeric)
    mtm_pct = Column(Numeric)
