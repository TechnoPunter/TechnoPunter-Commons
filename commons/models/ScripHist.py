# models.py

from sqlalchemy import Column, String, Integer, Numeric
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class ScripHist(Base):
    __tablename__ = "scrip_hist"

    scrip = Column(String, primary_key=True, index=True)
    time = Column(Integer, primary_key=True, index=True)
    time_frame = Column(String, primary_key=True, index=True)
    date = Column(String)
    open = Column(Numeric)
    high = Column(Numeric)
    low = Column(Numeric)
    close = Column(Numeric)
