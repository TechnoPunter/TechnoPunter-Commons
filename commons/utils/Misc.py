import datetime
import logging
import time

import pandas as pd

from commons.consts.consts import IST
from commons.dataprovider.database import DatabaseEngine

logger = logging.getLogger(__name__)
LOG_STORE_MODEL = "LogStore"


def get_epoch(date_string: str):
    if date_string == '0':
        return int(time.time())
    else:
        # Define the date string and format
        date_format = '%d-%m-%Y %H:%M:%S'
        return int(IST.localize(datetime.datetime.strptime(date_string, date_format)).timestamp())


def calc_sl(entry: float, signal: int, sl_factor: float, tick: float, scrip: str):
    logger.debug(f"Entered Calc SL with SL Factor: {sl_factor}, Tick: {tick}, Scrip: {scrip}")
    tick = float(tick)
    sl = float(entry) - signal * float(entry) * sl_factor / 100
    sl = format(round(sl / tick) * tick, ".2f")
    logger.debug(f"{scrip}: Calc SL: {sl}")
    return sl


def round_price(price: float, tick: float, scrip: str):
    logger.debug(f"Entered Round Price with : {price}, Tick: {tick}, Scrip: {scrip}")
    tick = float(tick)
    price = format(round(price / tick) * tick, ".2f")
    logger.debug(f"{scrip}: Calc Price: {price}")
    return price


def get_new_sl(order: dict, ltp: float = None):
    """

    Args:
        order:{
                "order_no": 1234,
                "scrip": "NSE_BANDHANBNK",
                "direction": -1 (of the trade)
                "open_qty": 10,
                "sl_price": 201.25,
                "entry_price": 200.05
                "remarks" : "exec.strategies.gspcV2:NSE_RELIANCE:2023-09..."}
        ltp: 200.55

    Returns:

    """
    logger.debug(f"__get_new_sl: Update order for {order['scrip']} for SL Order ID: {order['sl_order_id']}")
    direction = 1 if order['signal'] == 1 else -1

    sl = order['sl_pct']
    trail_sl = order['trail_sl_pct']
    logger.debug(f"get_new_sl: SL: {sl}; Trail SL: {trail_sl}")
    logger.debug(f"get_new_sl: Validating if "
                 f"{abs(ltp - float(order['sl_price']))} > {ltp * float((sl + trail_sl) / 100)}")
    if abs(ltp - float(order['sl_price'])) > ltp * float((sl + trail_sl) / 100):
        new_sl = ltp - direction * ltp * float(sl / 100)
        new_sl = format(round(new_sl / order['tick']) * order['tick'], ".2f")
        logger.debug(f"get_new_sl: Updated sl: {new_sl}")
        return new_sl
    else:
        logger.info(f"get_new_sl: Same sl for {order['scrip']} @ {ltp}")
        return "0.0"


def log_entry(trader_db: DatabaseEngine, log_type: str, keys: list[str], data):
    """
    Makes entry in LogStore table - Also takes care of NaN in dict
    {
        "log_key": {log_type}_{keys},
        "log_type": log_type,
        "log_data": JSON,
        "log_time": get_epoch(0)
    }
    """
    log_key = "_".join([log_type] + keys)
    if isinstance(data, pd.DataFrame):
        log_data = data.fillna(0).to_dict()
    else:
        log_data = data
    rec = {
        "log_key": log_key,
        "log_type": log_type,
        "log_data": log_data,
        "log_time": get_epoch("0")
    }
    trader_db.single_insert(LOG_STORE_MODEL, rec)
