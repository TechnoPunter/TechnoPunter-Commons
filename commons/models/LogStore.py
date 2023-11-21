from sqlalchemy import Column, Integer, String, Numeric
from sqlalchemy.dialects.postgresql import JSONB

from commons.dataprovider.database import Base


class LogStore(Base):
    __tablename__ = 'log_store'

    log_id = Column(Integer, primary_key=True, autoincrement=True)
    log_key = Column(String)
    log_type = Column(String)
    data = Column(JSONB)
    log_time = Column(Integer)