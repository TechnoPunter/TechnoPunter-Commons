from commons.consts.consts import SCRIP_HIST
from commons.dataprovider.database import DatabaseEngine


class ScripData:
    trader_db: DatabaseEngine

    def __init__(self):
        self.trader_db = DatabaseEngine()

    def get_scrip_data(self, scrip_name: str, time_frame: str = '1', from_date: str = '1900-01-01'):
        predicate = f"m.{SCRIP_HIST}.scrip == '{scrip_name}'"
        predicate += f",m.{SCRIP_HIST}.time_frame == '{time_frame}'"
        if from_date != '1900-01-01':
            predicate += f",m.{SCRIP_HIST}.date  >= '{from_date}'"
        return self.trader_db.query(SCRIP_HIST, predicate)
