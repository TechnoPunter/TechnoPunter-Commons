import pandas as pd

from commons.consts.consts import SCRIP_HIST, IST, Interval
from commons.dataprovider.database import DatabaseEngine


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
        return self.trader_db.query_df(SCRIP_HIST, predicate)

    def load_scrip_data(self, data: pd.DataFrame, scrip_name: str, time_frame: Interval = Interval.in_1_minute):
        df = data.copy()
        from_epoch = str(df.time.min())
        predicate = f"m.{SCRIP_HIST}.scrip == '{scrip_name}'"
        predicate += f",m.{SCRIP_HIST}.time_frame == '{time_frame.value}'"
        predicate += f",m.{SCRIP_HIST}.time  >= '{from_epoch}'"
        self.trader_db.delete_recs(SCRIP_HIST, predicate=predicate)

        df['date'] = pd.to_datetime(df['time'].astype(int), unit='s', utc=True)
        df['date'] = df['date'].dt.tz_convert(IST)
        df['date'] = df['date'].dt.date
        df['date'] = df['date'].astype(str)

        df.loc[:, 'scrip'] = scrip_name
        df.loc[:, 'time_frame'] = time_frame.value

        self.trader_db.bulk_insert(SCRIP_HIST, df)
        return "Ok"

    def get_base_data(self, scrip_name: str):
        df = self.get_scrip_data(scrip_name=scrip_name, time_frame=Interval.in_daily)
        return df[["time", "open", "high", "low", "close"]]

    def get_tick_data(self, scrip_name: str):
        df = self.get_scrip_data(scrip_name=scrip_name, time_frame=Interval.in_1_minute)
        return df[["time", "open", "high", "low", "close"]]


if __name__ == '__main__':
    sd = ScripData()
    x = sd.get_scrip_data(scrip_name='DUMMY')
    print(x)

    from commons.dataprovider.filereader import get_base_data, get_tick_data

    scrip = 'NSE_RELIANCE'
    df_ = get_base_data(scrip)
    x = sd.load_scrip_data(data=df_, scrip_name=scrip, time_frame=Interval.in_daily)

    df_ = get_tick_data(scrip)
    y = sd.load_scrip_data(data=df_, scrip_name=scrip, time_frame=Interval.in_1_minute)

    ret = sd.get_base_data(scrip)
    print(ret)
