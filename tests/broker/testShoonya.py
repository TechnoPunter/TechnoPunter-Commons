import logging
import os
import unittest

import pytest


if os.path.exists('/var/www/TechnoPunter-Commons'):
    REPO_PATH = '/var/www/TechnoPunter-Commons/'
else:
    REPO_PATH = '/Users/pralhad/Documents/99-src/98-trading/TechnoPunter-Commons/'

os.environ['RESOURCE_PATH'] = os.path.join(REPO_PATH, 'resources/config')
os.environ['GENERATED_PATH'] = os.path.join(REPO_PATH, 'dummy')

logger = logging.getLogger(__name__)
from commons.broker.Shoonya import Shoonya
from commons.dataprovider.filereader import get_tick_data

ACCT = 'Trader-V2-Pralhad'


class TestShoonya(unittest.TestCase):
    s = Shoonya(ACCT)
    scrip = 'NSE_UPL'

    @pytest.fixture(autouse=True)
    def cleanup(self):
        """ Gets a logged-in Session"""
        res = self.s.api_login()

    def test_login(self):
        res = self.s.api_login()
        self.assertIsNotNone(res)

    def test_get_hist_prices(self):
        res = self.s.api_get_hist_prices(self.scrip, num_days=20)
        self.assertIsNotNone(res)

    def test_get_base_data(self):
        res = self.s.get_base_data(self.scrip)
        self.assertIsNotNone(res)

    def test_get_time_series(self):
        res = self.s.api_get_time_series(self.scrip, num_days=20)
        self.assertIsNotNone(res)

    def test_get_tick_data(self):
        res = self.s.get_tick_data(self.scrip, num_days=100)
        self.assertIsNotNone(res)


if __name__ == "__main__":
    unittest.main()
