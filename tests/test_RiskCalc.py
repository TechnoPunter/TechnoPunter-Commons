import json
import os
import unittest

import pandas as pd

if os.path.exists('/var/www/trade-exec-engine/resources/test'):
    REPO_DIR = '/var/www/trade-exec-engine/resources/test'
else:
    REPO_DIR = '/Users/pralhad/Documents/99-src/98-trading/TechnoPunter-Commons'

ACCT = "Trader-V2-Pralhad"
TEST_RESOURCE_DIR = os.path.join(REPO_DIR, "resources/test")

os.environ["ACCOUNT"] = ACCT
os.environ["GENERATED_PATH"] = os.path.join(REPO_DIR, "generated")
os.environ["LOG_PATH"] = os.path.join(REPO_DIR, "logs")
os.environ["RESOURCE_PATH"] = os.path.join(TEST_RESOURCE_DIR, "risk-calc/config")

from commons.service.RiskCalc import RiskCalc


def read_file(name, ret_type: str = "JSON"):
    res_file_path = os.path.join(TEST_RESOURCE_DIR, name)
    with open(res_file_path, 'r') as file:
        result = file.read()
        if ret_type == "DF":
            return pd.DataFrame(json.loads(result))
        else:
            return json.loads(result)


def read_file_df(name):
    return read_file(name, ret_type="DF")


class TestRiskCalc(unittest.TestCase):
    rc = RiskCalc()

    def test_risk_calc(self):
        risk_params = self.rc.risk_params
        result = read_file("risk-calc/expected-risk-params.json")
        self.assertEqual(result, risk_params)

    def test_calc_risk_params(self):
        rc = RiskCalc()

        scrip = "NSE_APOLLOHOSP"
        strategy = "trainer.strategies.gspcV2"
        signal = 1
        tick = 0.05
        acct = "Trader-V2-Pralhad"
        entry = 100.00
        pred_target = 101.00

        t_r, sl_r = rc.calc_risk_params(scrip=scrip, strategy=strategy, signal=signal, tick=tick, acct=acct,
                                        entry=entry, pred_target=pred_target)

        self.assertEqual('1.10', t_r)
        self.assertEqual('0.70', sl_r)

    def test_calc_risk_params_acct(self):
        rc = RiskCalc()

        scrip = "NSE_APOLLOHOSP"
        strategy = "trainer.strategies.gspcV2"
        signal = 1
        tick = 0.05
        acct = "Trader-V2-Mahi"
        entry = 100.00
        pred_target = 101.00

        t_r, sl_r = rc.calc_risk_params(scrip=scrip, strategy=strategy, signal=signal, tick=tick, acct=acct,
                                        entry=entry, pred_target=pred_target)

        self.assertEqual('1.20', t_r)
        self.assertEqual('0.90', sl_r)

    def test_calc_risk_params_fallback(self):
        rc = RiskCalc()

        scrip = "X"
        strategy = "Y"
        signal = 1
        tick = 0.05
        acct = "X"
        entry = 100.00
        pred_target = 101.00

        t_r, sl_r = rc.calc_risk_params(scrip=scrip, strategy=strategy, signal=signal, tick=tick, acct=acct,
                                        entry=entry, pred_target=pred_target)

        self.assertEqual('1.00', t_r)
        self.assertEqual('1.00', sl_r)


if __name__ == '__main__':
    pass
