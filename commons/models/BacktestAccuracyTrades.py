from sqlalchemy import Column, Integer, String, Numeric

from commons.dataprovider.database import Base


class BacktestAccuracyTrades(Base):
    __tablename__ = 'bt_accuracy_trades'

    bt_accu_trade_id = Column(Integer, primary_key=True)
    
    run_type = Column(String)
    scrip = Column(String)
    strategy = Column(String)
    tick = Column(Numeric)
    date = Column(String)
    signal = Column(Numeric)
    target = Column(Numeric)
    bod_strength = Column(Numeric)
    bod_sl = Column(Numeric)
    sl_range = Column(Numeric)
    trail_sl = Column(Numeric)
    strength = Column(Numeric)
    entry_time = Column(Integer)
    entry_price = Column(Numeric)
    status = Column(String)
    exit_price = Column(Numeric)
    exit_time = Column(Integer)
    pnl = Column(Numeric)
    sl = Column(Numeric)
    sl_update_cnt = Column(Numeric)
    max_mtm = Column(Numeric)
    max_mtm_pct = Column(Numeric)
