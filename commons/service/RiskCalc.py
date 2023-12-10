import json
import logging

from commons.config.reader import cfg
from commons.utils.Misc import round_price

logger = logging.getLogger(__name__)


class RiskCalc:
    risk_params: dict
    default_reward_factor: float
    default_risk_reward_ratio: float
    default_trail_sl_factor: float

    def __init__(self):
        logger.debug(f"Starting RiskCalc")
        self.risk_params = {}
        self.__build_risk_params()

    def __build_risk_params(self):
        logger.debug(f"Starting __build_risk_params")
        r_cfg = cfg['risk-params']
        default_accounts = r_cfg.get('accounts')
        defaults = r_cfg.get('defaults')
        logger.debug(f"r_cfg:\n{r_cfg}\nDefaults:\n{defaults}")

        self.default_reward_factor = defaults.get('reward_factor')
        self.default_risk_reward_ratio = defaults.get('risk_reward_ratio')
        self.default_trail_sl_factor = defaults.get('trail_sl_factor')

        for scrip in r_cfg.get('scrips'):
            scrip_name = scrip.get('scripName')
            for model in scrip.get('models'):
                model_name = model.get('name')
                signal = str(model.get('signal'))
                reward_factor = model.get('reward_factor')
                risk_reward_ratio = model.get('risk_reward_ratio')
                trail_sl_factor = model.get('trail_sl_factor')
                override_accounts = []
                for acct in model.get('accounts'):
                    acct_name = acct.get('name')
                    key = ":".join([scrip_name, model_name, signal, acct_name])
                    acct_reward_factor = acct.get('reward_factor', reward_factor)
                    acct_risk_reward_ratio = acct.get('risk_reward_ratio', risk_reward_ratio)
                    acct_trail_sl_factor = acct.get('trail_sl_factor', trail_sl_factor)
                    self.risk_params[key] = {
                        "reward_factor": acct_reward_factor,
                        "risk_reward_ratio": acct_risk_reward_ratio,
                        "trail_sl_factor": acct_trail_sl_factor
                    }
                    override_accounts.append(acct.get('name'))
                for acct_name in set(default_accounts).difference(set(override_accounts)):
                    key = ":".join([scrip_name, model_name, signal, acct_name])
                    self.risk_params[key] = {
                        "reward_factor": reward_factor,
                        "risk_reward_ratio": risk_reward_ratio,
                        "trail_sl_factor": trail_sl_factor
                    }
        logger.info(f"Final Risk Params:\n{json.dumps(self.risk_params, indent=4, sort_keys=True)}")

    def calc_risk_params(self, scrip: str, strategy: str, signal: int, tick: float, acct: str, entry: float,
                         pred_target: float) -> (str, str):
        """
        Get RF & RRR for the A/c, Scrip, Strategy, direction.
        Example:
        entry = 100
        predicted target = 101
        signal = 1

        RF = 0.1 (i.e. +10%)
        RRR = 1.5 (i.e. Will risk 1 for earning 1.5)

        adjusted target range = (101 - 100) * 110% => 1.1

        rrr factor = 1 / 1.5 => 0.66
        adjusted sl range = 1.1 * 0.66 => 0.73

        Now applying rounding based on tick

        (ret_adj_target_range, ret_adj_sl_range) = (1.1, 0.75)

        :param acct:
        :param scrip:
        :param strategy:
        :param signal:
        :param tick:
        :param entry:
        :param pred_target:
        :return:
        """
        key = ":".join([scrip, strategy, str(signal), acct])
        logger.info(f"Key:{key}")
        rec = self.risk_params.get(key, {'reward_factor': self.default_reward_factor,
                                         'risk_reward_ratio': self.default_risk_reward_ratio})

        logger.debug(f"Rec:{rec}")

        reward_factor = rec.get('reward_factor', self.default_reward_factor)
        risk_reward_ratio = rec.get('risk_reward_ratio', self.default_risk_reward_ratio)
        trail_sl_factor = rec.get('trail_sl_factor', self.default_trail_sl_factor)

        target_range = abs(pred_target - entry)

        adj_target_range = target_range * (1 + reward_factor)

        adj_sl_range = adj_target_range / risk_reward_ratio
        adj_trail_sl_range = adj_sl_range * trail_sl_factor

        ret_adj_target_range = round_price(price=adj_target_range, tick=tick, scrip=scrip)
        ret_adj_sl_range = round_price(price=adj_sl_range, tick=tick, scrip=scrip)
        ret_adj_trail_sl_range = round_price(price=adj_trail_sl_range, tick=tick, scrip=scrip)
        logger.info(f"Target:{ret_adj_target_range} SL:{ret_adj_sl_range} Trail SL: {ret_adj_trail_sl_range}")
        return ret_adj_target_range, ret_adj_sl_range, ret_adj_trail_sl_range


if __name__ == '__main__':
    from commons.loggers.setup_logger import setup_logging

    setup_logging("risk-calc.log")
    rc = RiskCalc()
    _scrip = "X"
    _strategy = "Y"
    _signal = 1
    _tick = 0.05
    _acct = "X"
    _entry = 100.00
    _pred_target = 101.00
    x, y, z = rc.calc_risk_params(scrip=_scrip, strategy=_strategy, signal=_signal, tick=_tick, acct=_acct,
                               entry=_entry, pred_target=_pred_target)
    print(f"{x}, {y}, {z}")