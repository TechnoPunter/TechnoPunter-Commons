from Utils import *
from commons.loggers.setup_logger import setup_logging

setup_logging("risk-calc-test.log")

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


class TestRiskCalc(unittest.TestCase):

    def test_risk_calc(self):
        rc = RiskCalc(mode="PRESET")
        risk_params = rc.risk_params
        result = read_file("risk-calc/expected-risk-params.json")
        self.assertEqual(result, risk_params)

    def test_risk_calc_default(self):
        rc = RiskCalc(mode="DEFAULT")
        actual_rf = rc.default_reward_factor
        actual_rrr = rc.default_risk_reward_ratio
        actual_tsl = rc.default_trail_sl_factor
        self.assertEqual(0.0, actual_rf)
        self.assertEqual(1.0, actual_rrr)
        self.assertEqual(0.5, actual_tsl)

    def test_calc_risk_params_scrip_default(self):
        rc = RiskCalc(mode="PRESET")

        scrip = "NSE_APOLLOHOSP"
        strategy = "trainer.strategies.gspcV2"
        signal = 1
        tick = 0.05
        acct = "Trader-V2-Pralhad"
        prev_close = 100.00
        entry = 100.10
        pred_target = 101.00

        t_r, sl_r, t_sl_r = rc.calc_risk_params(scrip=scrip, strategy=strategy, signal=signal, tick=tick, acct=acct,
                                                prev_close=prev_close, entry=entry, pred_target=pred_target)

        self.assertEqual('1.00', t_r, "Reward is not matching")
        self.assertEqual('0.60', sl_r, "SL is not matching")
        self.assertEqual('0.25', t_sl_r, "T-SL is not matching")

    def test_calc_risk_params_acct(self):
        rc = RiskCalc(mode="PRESET")

        scrip = "NSE_APOLLOHOSP"
        strategy = "trainer.strategies.gspcV2"
        signal = 1
        tick = 0.05
        acct = "Trader-V2-Mahi"
        prev_close = 100.00
        entry = 100.10
        pred_target = 101.00

        t_r, sl_r, t_sl_r = rc.calc_risk_params(scrip=scrip, strategy=strategy, signal=signal, tick=tick, acct=acct,
                                                prev_close=prev_close, entry=entry, pred_target=pred_target)

        self.assertEqual('1.10', t_r, "Reward is not matching")
        self.assertEqual('0.35', sl_r, "SL is not matching")
        self.assertEqual('0.15', t_sl_r, "T-SL is not matching")

    def test_calc_risk_params_fallback(self):
        rc = RiskCalc(mode="PRESET")

        scrip = "X"
        strategy = "Y"
        signal = 1
        tick = 0.05
        acct = "X"
        prev_close = 100.00
        entry = 100.10
        pred_target = 101.00

        t_r, sl_r, t_sl_r = rc.calc_risk_params(scrip=scrip, strategy=strategy, signal=signal, tick=tick, acct=acct,
                                                prev_close=prev_close, entry=entry, pred_target=pred_target)

        self.assertEqual('0.90', t_r, "Reward is not matching")
        self.assertEqual('0.45', sl_r, "SL is not matching")
        self.assertEqual('0.15', t_sl_r, "T-SL is not matching")

    def test_calc_risk_params_accuracy(self):
        accu_df = read_file_df("risk-calc/Portfolio-Accuracy.csv")
        rc = RiskCalc(mode="PRESET", accuracy=accu_df)

        scrip = "NSE_APOLLOHOSP"
        strategy = "trainer.strategies.rfcV2"
        signal = 1
        trade_dt = "2023-10-05"
        tick = 0.05
        acct = "X"
        prev_close = 100.00
        entry = 100.10
        pred_target = 101.00

        t_r, sl_r, t_sl_r = rc.calc_risk_params(scrip=scrip, strategy=strategy, signal=signal, tick=tick, acct=acct,
                                                prev_close=prev_close, entry=entry, pred_target=pred_target,
                                                risk_date=trade_dt)

        self.assertEqual('3.65', t_r, "Reward is not matching")
        self.assertEqual('0.90', sl_r, "SL is not matching")
        self.assertEqual('0.25', t_sl_r, "T-SL is not matching")


if __name__ == '__main__':
    pass
