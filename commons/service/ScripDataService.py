import logging
import time

from commons.broker.Shoonya import Shoonya
from commons.config.reader import cfg
from commons.consts.consts import Interval
from commons.dataprovider.ScripData import ScripData
from commons.dataprovider.database import DatabaseEngine
from commons.loggers.setup_logger import setup_logging

logger = logging.getLogger(__name__)


class ScripDataService:
    s: Shoonya
    sd: ScripData

    def __init__(self, shoonya: Shoonya, trader_db: DatabaseEngine = None):
        self.s = shoonya
        self.sd = ScripData(trader_db=trader_db)

    def load_scrip_data(self, scrip_name, opts, num_days: int = 7):
        logger.info(f"Started loading for Scrip: {len(scrip_name)}")
        if "TICK" in opts:
            tick = self.s.get_tick_data(scrip_name=scrip_name, num_days=num_days)
            self.sd.save_scrip_data(data=tick, scrip_name=scrip_name, time_frame=Interval.in_1_minute)
            time.sleep(2)
        if "BASE" in opts:
            base = self.s.get_base_data(scrip_name=scrip_name, num_days=num_days)
            self.sd.save_scrip_data(data=base, scrip_name=scrip_name, time_frame=Interval.in_daily)

    def load_scrips_data(self, scrip_names: list[str], base_num_days: int = 7, tick_num_days: int = 7, opts=None):
        if opts is None:
            opts = ['TICK', 'BASE']
        logger.info(f"Started loading for {len(scrip_names)}")
        # Get both tick & base data for all scrips for resp no. of days
        prices = self.s.get_prices_data(scrip_names=scrip_names, opts=opts, base_num_days=base_num_days,
                                        tick_num_days=tick_num_days)
        logger.info(f"Retried data for {len(prices)} scrip, tf combinations")
        for scrip_name, interval, df in prices:
            logger.debug(f"Storing data for: {scrip_name} @ TF: {interval.value}")
            self.sd.save_scrip_data(data=df, scrip_name=scrip_name, time_frame=interval)


if __name__ == '__main__':
    setup_logging(log_file_name="sds.log")

    ACCT = 'Trader-V2-Pralhad'
    sh = Shoonya(ACCT)
    sds = ScripDataService(shoonya=sh)
    scrip = 'NSE_ONGC'
    sds.load_scrips_data(cfg['steps']['scrips'], base_num_days=800, tick_num_days=10, opts=["BASE", "TICK"])
