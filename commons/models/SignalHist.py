# models.py

from sqlalchemy import Column, String, Integer, Numeric, JSON
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class SignalHist(Base):
    __tablename__ = "signal_hist"

    scrip = Column(String, primary_key=True, index=True)
    timestamp = Column(Integer, primary_key=True, index=True)
    time_frame = Column(String, primary_key=True, index=True)
    signal_low = Column(Numeric)
    signal_high = Column(Numeric)
    indicators = Column(JSON)
