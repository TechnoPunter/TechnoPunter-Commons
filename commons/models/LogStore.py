from sqlalchemy import Column, Integer, String, Numeric
from sqlalchemy.dialects.postgresql import JSONB

from commons.dataprovider.database import Base


class LogStore(Base):
    __tablename__ = 'log_store'

    log_id = Column(Integer, primary_key=True)
    log_key = Column(String)
    log_type = Column(String)
    log_data = Column(JSONB)
    acct = Column(String)
    log_date = Column(String)
    log_time = Column(Integer)