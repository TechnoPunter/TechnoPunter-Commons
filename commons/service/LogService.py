import logging

import pandas as pd

from commons.consts.consts import LOG_STORE_MODEL
from commons.dataprovider.database import DatabaseEngine
from commons.utils.Misc import get_epoch

logger = logging.getLogger(__name__)


class LogService:
    def __init__(self, trader_db: DatabaseEngine = None):
        if trader_db is None:
            self.trader_db = DatabaseEngine()
        else:
            self.trader_db = trader_db

    def log_entry(self, log_type: str, keys: list[str], acct, log_date, data):
        """
        Makes entry in LogStore table - Also takes care of NaN in dict
        {
            "log_key": {log_type}_{keys},
            "log_type": log_type,
            "log_data": JSON,
            "log_time": get_epoch(0)
        }
        """
        key_list = [log_type] + keys
        key_list.append(log_date)
        key_list.append(acct)
        log_key = "_".join(key_list)
        if isinstance(data, pd.DataFrame):
            log_data = data.fillna(0).to_dict(orient="records")
        else:
            log_data = data
        rec = {
            "log_key": log_key,
            "log_type": log_type,
            "log_data": log_data,
            "log_time": get_epoch("0"),
            "log_date": log_date,
            "acct": acct
        }
        self.trader_db.delete_recs(table=LOG_STORE_MODEL, predicate=f"m.{LOG_STORE_MODEL}.log_key == '{log_key}'")
        self.trader_db.single_insert(LOG_STORE_MODEL, rec)

    def get_log_entry_df(self, log_type: str, keys: list[str], acct, log_date):
        key_list = [log_type] + keys
        key_list.append(log_date)
        key_list.append(acct)
        log_key = "_".join(key_list)
        ret = self.trader_db.query_df(table=LOG_STORE_MODEL, predicate=f"m.{LOG_STORE_MODEL}.log_key == '{log_key}'")
        if len(ret) == 0:

            return None
        else:
            return ret

    def get_log_entry_data(self, log_type: str, keys: list[str], acct, log_date):
        df = self.get_log_entry_df(log_type=log_type, keys=keys, acct=acct, log_date=log_date)
        if df is None:
            return None
        else:
            data = df.iloc[0]['log_data']
            return data


if __name__ == '__main__':
    from commons.loggers.setup_logger import setup_logging

    setup_logging("LogService.log")

    ls = LogService()
    data_ = ls.get_log_entry_df("Params", ["COB"], "Trader-V2-Mahi", "2023-11-28")
    print(data_)
    data_ = ls.get_log_entry_data("Params", ["COB"], "Trader-V2-Mahi", "2023-11-28")
    print(data_)
