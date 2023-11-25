import logging
import time

from commons.broker.Shoonya import Shoonya
from commons.config.reader import cfg
from commons.dataprovider.ScripData import ScripData
from commons.dataprovider.tvfeed import Interval

logger = logging.getLogger(__name__)

ACCT = 'Trader-V2-Pralhad'


class ScripDataService:
    s: Shoonya
    sd: ScripData

    def __init__(self):
        self.s = Shoonya(acct=ACCT)
        self.sd = ScripData()

    def load_scrip_data(self, scrip_name, num_days: int = 7):
        logger.info(f"Started loading for Scrip: {len(scrip_name)}")
        tick = self.s.get_tick_data(scrip_name=scrip_name, num_days=num_days)
        self.sd.load_scrip_data(data=tick, scrip_name=scrip_name, time_frame=Interval.in_1_minute)
        time.sleep(2)
        base = self.s.get_base_data(scrip_name=scrip_name, num_days=num_days)
        self.sd.load_scrip_data(data=base, scrip_name=scrip_name, time_frame=Interval.in_daily)

    def load_scrips_data(self, scrip_names: list[str], num_days: int = 7):
        logger.info(f"Started loading for {len(scrip_names)}")
        for scrip_name in scrip_names:
            self.load_scrip_data(scrip_name=scrip_name, num_days=num_days)
            time.sleep(1)


if __name__ == '__main__':
    sds = ScripDataService()
    scrip = 'NSE_ONGC'
    sds.load_scrips_data(cfg['steps']['scrips'], num_days=180)
