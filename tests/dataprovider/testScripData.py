import logging
import os
import unittest

import pandas as pd
import pytest

if os.path.exists('/var/www/TechnoPunter-Commons'):
    REPO_PATH = '/var/www/TechnoPunter-Commons/'
    TEST_RESOURCE_DIR = '/var/www/TechnoPunter-Commons/commons/resources/test'
else:
    REPO_PATH = '/Users/pralhad/Documents/99-src/98-trading/TechnoPunter-Commons/'
    TEST_RESOURCE_DIR = "/Users/pralhad/Documents/99-src/98-trading/TechnoPunter-Commons/commons/resources/test"

os.environ['RESOURCE_PATH'] = os.path.join(REPO_PATH, 'resources/config')
os.environ['GENERATED_PATH'] = os.path.join(REPO_PATH, 'dummy')

logger = logging.getLogger(__name__)

from commons.dataprovider.ScripData import ScripData
from commons.dataprovider.tvfeed import Interval
from commons.consts.consts import SCRIP_HIST


class TestScripData(unittest.TestCase):
    sd = ScripData()

    rec1 = [{
        "time": 1000,
        "open": 100.1,
        "high": 100.2,
        "low": 100.0,
        "close": 100.1
    }]

    rec2 = [{
        "time": 1000,
        "open": 100.1,
        "high": 100.2,
        "low": 100.0,
        "close": 100.3
    }]

    rec3 = [
        {
            "time": 1000,
            "open": 100.1,
            "high": 100.2,
            "low": 100.0,
            "close": 100.3
        },
        {
            "time": 1001,
            "open": 100.1,
            "high": 100.2,
            "low": 100.0,
            "close": 100.3
        }
    ]

    rec4 = [
        {
            "time": 1002,
            "open": 100.1,
            "high": 100.2,
            "low": 100.0,
            "close": 100.3
        },
        {
            "time": 1003,
            "open": 100.1,
            "high": 100.2,
            "low": 100.0,
            "close": 100.3
        }
    ]

    dummy_scrip = 'DUMMY'
    clean_predicate = f"m.{SCRIP_HIST}.scrip == '{dummy_scrip}'"

    @pytest.fixture(autouse=True)
    def cleanup(self):
        """ Cleans up Dummy records from DB"""
        self.sd.trader_db.delete_recs(SCRIP_HIST, predicate=self.clean_predicate)

    def test_record_load(self):
        data = pd.DataFrame(self.rec1)
        res = self.sd.load_scrip_data(data=data, scrip_name=self.dummy_scrip, time_frame=Interval.in_1_minute)
        self.assertIsNotNone(res)

    def test_record_get(self):
        data = pd.DataFrame(self.rec1)
        self.sd.load_scrip_data(data=data, scrip_name=self.dummy_scrip, time_frame=Interval.in_1_minute)

        data.loc[:, 'scrip'] = self.dummy_scrip
        data.loc[:, 'time_frame'] = Interval.in_1_minute.value
        res = self.sd.get_scrip_data(self.dummy_scrip)
        res.drop(columns=["date"], inplace=True)

        self.assertGreaterEqual(1, len(res))
        pd.testing.assert_frame_equal(data.sort_index(axis=1), res.sort_index(axis=1))

    def test_get_base_data(self):
        data = pd.DataFrame(self.rec3)
        self.sd.load_scrip_data(data=data, scrip_name=self.dummy_scrip, time_frame=Interval.in_daily)

        res = self.sd.get_base_data(self.dummy_scrip)
        self.assertGreaterEqual(2, len(res))
        pd.testing.assert_frame_equal(data, res)

    def test_get_tick_data(self):
        data = pd.DataFrame(self.rec4)
        self.sd.load_scrip_data(data=data, scrip_name=self.dummy_scrip, time_frame=Interval.in_1_minute)

        res = self.sd.get_tick_data(self.dummy_scrip)
        self.assertGreaterEqual(2, len(res))
        pd.testing.assert_frame_equal(data, res)


if __name__ == "__main__":
    unittest.main()
