from sqlalchemy import Column, Integer, String, Numeric

from commons.dataprovider.database import Base


class BacktestAccuracy(Base):
    __tablename__ = 'backtest_accuracy'

    bt_accu_id = Column(Integer, primary_key=True)

    run_type = Column(String)
    scrip = Column(String)
    strategy = Column(String)
    trade_date = Column(String)
    trades = Column(Integer)
    pct_entry = Column(Numeric)
    l_num_predictions = Column(Integer)
    l_num_trades = Column(Integer)
    l_pct_success = Column(Numeric)
    l_pnl = Column(Numeric)
    l_avg_cost = Column(Numeric)
    l_pct_returns = Column(Numeric)
    l_pct_entry = Column(Numeric)
    l_reward_factor = Column(Numeric)
    s_num_predictions = Column(Integer)
    s_num_trades = Column(Integer)
    s_pct_success = Column(Numeric)
    s_pnl = Column(Numeric)
    s_avg_cost = Column(Numeric)
    s_pct_returns = Column(Numeric)
    s_pct_entry = Column(Numeric)
    s_reward_factor = Column(Numeric)
