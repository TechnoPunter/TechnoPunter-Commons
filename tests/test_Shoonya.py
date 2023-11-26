import json
import logging
import os
import unittest

import pandas as pd
import pytest

if os.path.exists('/var/www/TechnoPunter-Commons'):
    REPO_PATH = '/var/www/TechnoPunter-Commons/'
else:
    REPO_PATH = '/Users/pralhad/Documents/99-src/98-trading/TechnoPunter-Commons/'

os.environ['RESOURCE_PATH'] = os.path.join(REPO_PATH, 'resources/config')
os.environ['GENERATED_PATH'] = os.path.join(REPO_PATH, 'dummy')

TEST_RESOURCE_DIR = os.path.join(REPO_PATH, 'resources/test')

logger = logging.getLogger(__name__)
from commons.broker.Shoonya import Shoonya

ACCT = 'Trader-V2-Pralhad'


def read_file(name, ret_type: str = "DF"):
    res_file_path = os.path.join(TEST_RESOURCE_DIR, name)
    with open(res_file_path, 'r') as file:
        result = file.read()
        if ret_type == "DF":
            return pd.DataFrame(json.loads(result))
        else:
            return json.loads(result)


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

    def test_get_order_type_order_book(self):
        ob_data = read_file("bo/order-book-cob.json", ret_type="JSON")
        ob_expected = read_file("bo/expected/order-book-cob-order-type.json", ret_type="JSON")
        result = self.s.get_order_type_order_book(order_book=ob_data)

        self.assertEqual(len(result), len(ob_data))
        self.assertIsNotNone(result)
        result_df = pd.DataFrame(result)
        expected_df = pd.DataFrame(ob_expected)
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_get_order_type_order_update(self):
        ob_data = read_file("bo/bo-entry-order-update.json", ret_type="JSON")
        ob_expected = read_file("bo/expected/bo-order-update-order-type.json", ret_type="JSON")
        results = []
        for message in ob_data:
            results.append(self.s.get_order_type_order_update(message=message))

        self.assertEqual(len(results), len(ob_data))
        result_df = pd.DataFrame(results)
        expected_df = pd.DataFrame(ob_expected)
        pd.testing.assert_frame_equal(result_df, expected_df)


if __name__ == "__main__":
    unittest.main()
