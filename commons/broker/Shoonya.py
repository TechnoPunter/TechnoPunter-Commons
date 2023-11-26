import datetime
import json
import logging
import os
import shutil
import signal
import subprocess
from urllib.request import urlopen

import pandas as pd
import pyotp
from NorenRestApiPy.NorenApi import NorenApi

from commons.config.reader import cfg
from commons.loggers.setup_logger import setup_logging
from commons.utils.EmailAlert import send_email
from commons.utils.Misc import get_bod_epoch

logger = logging.getLogger(__name__)

MOCK = False
SYMBOL_MASTER = "https://api.shoonya.com/NSE_symbols.txt.zip"
VALID_ORDER_STATUS = ['OPEN', 'TRIGGER_PENDING', 'COMPLETE', 'CANCELED']
SCRIP_MAP = {'BAJAJ_AUTO-EQ': 'BAJAJ-AUTO-EQ', 'M_M-EQ': 'M&M-EQ'}


def get_order_type(message):
    return message.get('remarks', 'NA').split(":")[0]


def alarm_handler(signum, frame):
    print(f"ALARM signal received {signum} at {frame}")
    raise Exception()


signal.signal(signal.SIGALRM, alarm_handler)


class Shoonya:
    acct: str
    api = NorenApi(host='https://api.shoonya.com/NorenWClientTP/',
                   websocket='wss://api.shoonya.com/NorenWSTP/')

    def __init__(self, acct):
        self.acct = acct
        self.creds = cfg['shoonya'][self.acct]
        self.api_login()
        self.__generate_reminders()
        self.symbols = self.__load_symbol_tokens()

    @staticmethod
    def __load_symbol_tokens():
        zip_file_name = 'NSE_symbols.zip'
        token_file_name = 'NSE_symbols.txt'
        # extracting zipfile from URL
        with urlopen(SYMBOL_MASTER) as response, open(zip_file_name, 'wb') as out_file:
            shutil.copyfileobj(response, out_file)

            # extracting required file from zipfile
            command = 'unzip -o ' + zip_file_name
            subprocess.call(command, shell=True)

        # deleting the files
        os.remove(zip_file_name)
        os.remove(token_file_name)

        # loading data from the file
        return pd.read_csv(token_file_name)

    def __generate_reminders(self):
        if self.creds.get('expiry_date', datetime.date.today()) <= datetime.date.today():
            send_email(body=f"Shoonya password expired for {self.acct} on {self.creds['expiry_date']}!!!",
                       subject="ERROR: Password Change Needed")

    def __get_token(self, scrip):
        logger.debug(f"Getting token for {scrip}")
        scrip = SCRIP_MAP.get(scrip, scrip)
        return str(self.symbols.loc[self.symbols.TradingSymbol == scrip]['Token'].iloc[0])

    def api_login(self):
        cred = self.creds
        logger.debug(f"api_login: About to call api.login with {cred}")
        resp = self.api.login(userid=cred['user'],
                              password=cred['pwd'],
                              twoFA=pyotp.TOTP(cred['token']).now(),
                              vendor_code=cred['vc'],
                              api_secret=cred['apikey'],
                              imei=cred['imei'])
        logger.debug(f"api_login: Post api.login; Resp: {resp}")
        return resp

    def api_get_order_book(self):
        logger.debug(f"api_get_order_book: About to call api.get_order_book")
        if MOCK:
            logger.debug("api_get_order_book: Sending Mock Response")
            return None
        resp = self.api.get_order_book()
        if resp is None:
            logger.error("api_get_order_book: Retrying!")
            self.api_login()
            resp = self.api.get_order_book()
        logger.debug(f"api_get_order_book: Resp from api.get_order_book {resp}")
        return resp

    def api_get_order_hist(self, order_no):
        logger.debug(f"api_get_order_hist: About to call api.api_get_order_hist for {order_no}")
        if MOCK:
            logger.debug("api_get_order_hist: Sending Mock Response")
            return "COMPLETE", "NA", 123.45
        resp = self.api.single_order_history(orderno=order_no)
        if resp is None:
            logger.error("api_get_order_hist: Retrying!")
            self.api_login()
            resp = self.api.single_order_history(orderno=order_no)
        if len(resp) == 0:
            logger.error(f"api_get_order_hist: Unable to get response from single_order_history")
            return "REJECTED", "NA", float(0.0)
        logger.debug(f"api_get_order_hist: Resp from api.single_order_history {resp}")
        ord_hist = pd.DataFrame(resp)
        rej = ord_hist.loc[ord_hist['status'] == 'REJECTED']
        if len(rej) > 0:
            order_status = "REJECTED"
            reject_reason = ord_hist.loc[ord_hist['status'] == 'REJECTED'].iloc[0]['rejreason']
            price = float(0.0)
        else:
            # Handle the off chance that order is pending
            order_rec = ord_hist.iloc[0]
            order_type = get_order_type(order_rec)
            if (order_rec.status == "PENDING") or (order_type == "ENTRY_LEG" and order_rec.status == "OPEN"):
                logger.warning(f"Order {order_no} is pending - Retrying")
                resp = self.api.single_order_history(orderno=order_no)
                ord_hist = pd.DataFrame(resp)
            if len(resp) == 0:
                logger.error(f"api_get_order_hist: Unable to get response from single_order_history")
                return "REJECTED", "NA", float(0.0)
            valid = ord_hist.loc[ord_hist.status.isin(VALID_ORDER_STATUS)]
            if len(valid) > 0:
                order_status = valid.iloc[0].status
                reject_reason = "NA"
                price = float(valid.iloc[0].get('avgprc', 0))
            else:
                order_status = "REJECTED"
                reject_reason = 'NA'
                price = float(0.0)
        logger.debug(f"api_get_order_hist: Status: {order_status}, Reason: {reject_reason}")
        return order_status, reject_reason, price

    def api_place_order(self,
                        buy_or_sell,
                        product_type,
                        exchange,
                        trading_symbol,
                        quantity,
                        disclose_qty,
                        price_type,
                        price,
                        trigger_price,
                        retention,
                        remarks):
        logger.debug(f"api_place_order: About to call api.place_order with {remarks}")
        if MOCK:
            logger.debug("api_place_order: Sending Mock Response")
            return dict(json.loads('{"request_time": "09:15:01 01-01-2023", "stat": "Ok", "norenordno": "1234"}'))
        resp = self.api.place_order(buy_or_sell=buy_or_sell,
                                    product_type=product_type,
                                    exchange=exchange,
                                    tradingsymbol=SCRIP_MAP.get(trading_symbol, trading_symbol),
                                    quantity=quantity,
                                    discloseqty=disclose_qty,
                                    price_type=price_type,
                                    price=price,
                                    trigger_price=trigger_price,
                                    retention=retention,
                                    remarks=remarks
                                    )
        if resp is None:
            logger.error(f"api_place_order: Retrying! for {remarks}")
            self.api_login()
            resp = self.api.place_order(buy_or_sell=buy_or_sell,
                                        product_type=product_type,
                                        exchange=exchange,
                                        tradingsymbol=SCRIP_MAP.get(trading_symbol, trading_symbol),
                                        quantity=quantity,
                                        discloseqty=disclose_qty,
                                        price_type=price_type,
                                        price=price,
                                        trigger_price=trigger_price,
                                        retention=retention,
                                        remarks=remarks
                                        )
        logger.debug(f"api_place_order: Resp from api.place_order {resp} with {remarks}")
        return resp

    def api_modify_order(self, order_no, exchange, trading_symbol, new_quantity, new_price_type,
                         new_trigger_price=None):
        logger.debug(f"api_modify_order: About to call api.modify_order for {trading_symbol} with "
                     f"{new_price_type} @ {new_trigger_price}")

        if MOCK:
            logger.debug("api_modify_order: Sending Mock Response")
            return dict(json.loads('{"request_time": "09:15:01 01-01-2023", "stat": "Ok", "result": "1234"}'))

        resp = self.api.modify_order(orderno=order_no,
                                     exchange=exchange,
                                     tradingsymbol=SCRIP_MAP.get(trading_symbol, trading_symbol),
                                     newquantity=new_quantity,
                                     newprice_type=new_price_type,
                                     newtrigger_price=new_trigger_price)
        if resp is None:
            logger.error(
                f"api_modify_order: Retrying! for {trading_symbol} with {new_price_type} @ {new_trigger_price}")
            self.api_login()
            resp = self.api.modify_order(orderno=order_no,
                                         exchange=exchange,
                                         tradingsymbol=SCRIP_MAP.get(trading_symbol, trading_symbol),
                                         newquantity=new_quantity,
                                         newprice_type=new_price_type,
                                         newtrigger_price=new_trigger_price)
        logger.debug(f"api_modify_order: Resp from api.modify_order for {trading_symbol} with  "
                     f"{new_price_type} @ {new_trigger_price} : {resp}")
        return resp

    def api_cancel_order(self, order_no):
        logger.debug(f"api_cancel_order: About to call api.cancel_order for {order_no}")
        if MOCK:
            logger.debug("api_cancel_order: Sending Mock Response")
            return dict(json.loads('{"request_time": "09:15:01 01-01-2023", "stat": "Ok", "result": "1234"}'))
        resp = self.api.cancel_order(order_no)
        if resp is None:
            logger.error(f"api_cancel_order: Retrying! for {order_no}")
            self.api_login()
            resp = self.api.cancel_order(order_no)
        logger.debug(f"api_cancel_order: Resp from api.cancel_order {resp} for {order_no}")
        return resp

    @staticmethod
    def __format_result(recs, time_format="date"):
        df = pd.DataFrame(recs)
        df.rename(columns={"into": "open", "inth": "high", "intl": "low", "intc": "close"}, inplace=True)
        if time_format == "date":
            # Daily data comes with 00:00:00 time
            df['time'] = df.ssboe.apply(lambda r: get_bod_epoch(str(datetime.datetime.fromtimestamp(int(r)).date())))
        else:
            # Time Series data comes with UTC aware timestamps
            df.rename(columns={"time": "old_time", "ssboe": "time"}, inplace=True)
        df.sort_values(by=['time'], inplace=True)
        return df[["time", "open", "high", "low", "close"]]

    def api_get_hist_prices(self, scrip_name, num_days: int = 7):
        exchange = scrip_name.split("_")[0]
        symbol = scrip_name.replace(exchange + "_", "") + "-EQ"
        symbol = SCRIP_MAP.get(symbol, symbol)
        # symbol = urllib.parse.quote(symbol)
        start_date = datetime.date.today() - datetime.timedelta(days=num_days)
        result = None
        signal.alarm(10)
        try:
            result = self.api.get_daily_price_series(exchange, symbol, startdate=get_bod_epoch(str(start_date)))
        except Exception as ex:
            print(ex)
        signal.alarm(0)
        return result

    def get_base_data(self, scrip_name, num_days: int = 800):
        recs = []
        prices = None
        while prices is None:
            print(f"Getting base data for {scrip_name}")
            prices = self.api_get_hist_prices(scrip_name, num_days)
        print(len(prices))
        for price in prices:
            recs.append(json.loads(price))

        df = self.__format_result(recs)
        return df

    def api_get_time_series(self, scrip_name, num_days: int = 7):
        exchange = scrip_name.split("_")[0]
        symbol = scrip_name.replace(exchange + "_", "") + "-EQ"
        token = self.__get_token(symbol)
        start_date = datetime.date.today() - datetime.timedelta(days=num_days)
        result = None
        signal.alarm(30)
        try:
            result = self.api.get_time_price_series(exchange, token, starttime=get_bod_epoch(str(start_date)))
        except Exception as ex:
            print(ex)
        signal.alarm(0)
        return result

    def get_tick_data(self, scrip_name, num_days: int = 10):
        prices = None
        while prices is None:
            print(f"Getting tick data for {scrip_name}")
            prices = self.api_get_time_series(scrip_name, num_days)
        print(len(prices))

        df = self.__format_result(prices, time_format="datetime")
        return df


if __name__ == '__main__':
    setup_logging()

    MOCK = True

    ACCT = 'Trader-V2-Pralhad'

    s = Shoonya(acct=ACCT)
    ob = s.api_get_order_book()
    print(ob)
    # scrip_ = 'NSE_M_M'
    # scrip_ = 'NSE_BAJAJ_AUTO'
    scrip_ = 'NSE_ONGC'
    x = s.get_base_data(scrip_)
    print(x)
