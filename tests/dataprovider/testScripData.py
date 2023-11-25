import logging
import os
import unittest
from decimal import Decimal

import pandas as pd

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


class TestScripData(unittest.TestCase):
    sd = ScripData()

    rec1 = [{
        "time": "1000",
        "open": 100.1,
        "high": 100.2,
        "low": 100.0,
        "close": 100.1
    }]

    rec2 = [{
        "time": "1000",
        "open": 100.1,
        "high": 100.2,
        "low": 100.0,
        "close": 100.3
    }]

    def test_record_load(self):
        data = pd.DataFrame(self.rec1)
        res = self.sd.load_scrip_data(data=data, scrip_name="DUMMY", time_frame=Interval.in_1_minute)
        self.assertIsNotNone(res)

    def test_record_get(self):
        data = pd.DataFrame(self.rec1)
        self.sd.load_scrip_data(data=data, scrip_name="DUMMY", time_frame=Interval.in_1_minute)

        res = self.sd.get_scrip_data("DUMMY")
        self.assertGreaterEqual(1, len(res))
        self.assertEqual(Decimal('100.1'), res[0].close)

        data = pd.DataFrame(self.rec2)
        self.sd.load_scrip_data(data=data, scrip_name="DUMMY", time_frame=Interval.in_1_minute)
        self.assertGreaterEqual(1, len(res))
        self.assertEqual(Decimal('100.3'), res[0].close)


if __name__ == "__main__":
    unittest.main()
