import pandas as pd

from commons.consts.consts import SCRIP_HIST, IST
from commons.dataprovider.database import DatabaseEngine
from commons.dataprovider.tvfeed import Interval


class ScripData:
    trader_db: DatabaseEngine

    def __init__(self):
        self.trader_db = DatabaseEngine()

    def get_scrip_data(self, scrip_name: str, time_frame: Interval = Interval.in_1_minute,
                       from_date: str = '1900-01-01'):
        predicate = f"m.{SCRIP_HIST}.scrip == '{scrip_name}'"
        predicate += f",m.{SCRIP_HIST}.time_frame == '{time_frame.value}'"
        if from_date != '1900-01-01':
            predicate += f",m.{SCRIP_HIST}.date  >= '{from_date}'"
        return self.trader_db.query(SCRIP_HIST, predicate)

    def load_scrip_data(self, data: pd.DataFrame, scrip_name: str, time_frame: Interval = Interval.in_1_minute):
        from_epoch = str(data.time.min())
        predicate = f"m.{SCRIP_HIST}.scrip == '{scrip_name}'"
        predicate += f",m.{SCRIP_HIST}.time_frame == '{time_frame.value}'"
        predicate += f",m.{SCRIP_HIST}.timestamp  >= '{from_epoch}'"
        self.trader_db.delete_recs(SCRIP_HIST, predicate=predicate)

        data['date'] = pd.to_datetime(data['time'], unit='s', utc=True)
        data['date'] = data['date'].dt.tz_convert(IST)
        data['date'] = data['date'].dt.date
        data['date'] = data['date'].astype(str)

        data.rename(columns={"time": "timestamp"}, inplace=True)
        data.loc[:, 'scrip'] = scrip_name
        data.loc[:, 'time_frame'] = time_frame.value

        self.trader_db.bulk_insert(SCRIP_HIST, data)
        return "Ok"


if __name__ == '__main__':
    sd = ScripData()
    x = sd.get_scrip_data(scrip_name='DUMMY')
    print(x)

    from commons.dataprovider.filereader import get_base_data, get_tick_data

    scrip = 'NSE_RELIANCE'
    df = get_base_data(scrip)
    x = sd.load_scrip_data(data=df, scrip_name=scrip, time_frame=Interval.in_1_minute)

    df = get_tick_data(scrip)
    y = sd.load_scrip_data(data=df, scrip_name=scrip, time_frame=Interval.in_daily)

