# models.py

from sqlalchemy import Column, String, Float
from sqlalchemy.sql import text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class SignalSQL(Base):
    __tablename__ = "signals"

    scrip = Column(String, primary_key=True, index=True)
    low = Column(Float)
    high = Column(Float)


# Custom SQL for database initialization
create_signals_table_sql = text(
    '''
    CREATE TABLE IF NOT EXISTS signals (
        scrip VARCHAR(255) PRIMARY KEY,
        low FLOAT,
        high FLOAT
    )
    '''
)
