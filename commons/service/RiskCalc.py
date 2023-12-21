import json
import logging

import pandas as pd

from commons.config.reader import cfg
from commons.utils.Misc import round_price

logger = logging.getLogger(__name__)


class RiskCalc:
    risk_params: dict
    accuracy: pd.DataFrame
    default_reward_factor: float
    default_risk_reward_ratio: float
    default_trail_sl_factor: float

    def __init__(self, mode: str = "DEFAULT", accuracy: pd.DataFrame = None):
        """
        :param mode: Can be DEFAULT for Risk Param defaults or PRESET for individually set Risk params
        """
        logger.debug(f"Starting RiskCalc")
        self.risk_params = {}
        self.__build_risk_params(mode)
        if accuracy is None:
            self.accuracy = pd.DataFrame()
        else:
            self.accuracy = self.__build_accuracy_params(accuracy)

    def __build_accuracy_params(self, accu_df: pd.DataFrame):
        """
        Split accuracy df rows to long & short metrics
        """
        l_cols = ['scrip', 'strategy', 'trade_date', 'l_pct_success', 'l_reward_factor']
        s_cols = ['scrip', 'strategy', 'trade_date', 's_pct_success', 's_reward_factor']
        comb_accu_df = pd.concat([
            accu_df[l_cols].assign(signal='1').rename(columns={'l_pct_success': 'pct_success',
                                                               'l_reward_factor': 'reward_factor'}),
            accu_df[s_cols].assign(signal='-1').rename(columns={'s_pct_success': 'pct_success',
                                                                's_reward_factor': 'reward_factor'})
        ])
        comb_accu_df['key'] = (comb_accu_df[['scrip', 'strategy', 'signal', 'trade_date']].agg(':'.join, axis=1))
        comb_accu_df.set_index(keys='key', inplace=True)
        comb_accu_df = comb_accu_df.assign(
            risk_reward_ratio=comb_accu_df.pct_success / 100,
            trail_sl_factor=self.default_trail_sl_factor
        )
        comb_accu_df.drop(columns=['scrip', 'strategy', 'trade_date', 'pct_success', 'signal'], inplace=True)
        comb_accu_df.fillna(0, inplace=True)
        return comb_accu_df

    def __build_risk_params(self, mode: str):
        logger.debug(f"Starting __build_risk_params")
        if mode == "DEFAULT":
            r_cfg = cfg['risk-param-defaults']
        else:
            r_cfg = cfg['risk-params']
        default_accounts = r_cfg.get('accounts')
        defaults = r_cfg.get('defaults')
        logger.debug(f"r_cfg:\n{r_cfg}\nDefaults:\n{defaults}")

        self.default_reward_factor = defaults.get('reward_factor')
        self.default_risk_reward_ratio = defaults.get('risk_reward_ratio')
        self.default_trail_sl_factor = defaults.get('trail_sl_factor')

        for scrip in r_cfg.get('scrips', []):
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

    @staticmethod
    def __is_valid(signal, pred_target, entry):
        if (signal == 1 and pred_target > entry) or (signal == -1 and pred_target < entry):
            return True
        else:
            return False

    def calc_risk_params(self, scrip: str, strategy: str, signal: int, tick: float, acct: str, entry: float,
                         pred_target: float, risk_date: str = None) -> (str, str):
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

        Edge Case Handling:
        In case we have received an invalid target i.e. -->
        entry = 100
        target = 98

        We will return safe values to prevent impact.

        :param acct:
        :param scrip:
        :param strategy:
        :param signal:
        :param tick:
        :param entry:
        :param pred_target:
        :param risk_date: Date for which we need the RF, RRR & T-SL Values
        :return:
        """
        if not (self.__is_valid(signal=signal, pred_target=pred_target, entry=entry)):
            logger.info(f"Invalid entry for {scrip} & {strategy} : {signal}; {pred_target}; LTP: {entry}")
            factor = -1 * abs(pred_target - entry)
            return round_price(factor, tick=tick, scrip=scrip), "0.00", "0.00"

        if risk_date is not None:
            key = ":".join([scrip, strategy, str(signal), risk_date])
            logger.debug(f"Accuracy based Key: {key}")

            try:
                accuracy_rec = self.accuracy.loc[key]
            except KeyError:
                key = ":".join([scrip, strategy, str(signal), acct])
                logger.debug(f"Using fallback key: {key}")
                accuracy_rec = self.risk_params.get(key, {'reward_factor': self.default_reward_factor,
                                                          'risk_reward_ratio': self.default_risk_reward_ratio})
            logger.debug(f"Rec:{accuracy_rec}")

            reward_factor = accuracy_rec.get('reward_factor', self.default_reward_factor)
            risk_reward_ratio = accuracy_rec.get('risk_reward_ratio', self.default_risk_reward_ratio)
            trail_sl_factor = accuracy_rec.get('trail_sl_factor', self.default_trail_sl_factor)

        else:
            key = ":".join([scrip, strategy, str(signal), acct])
            logger.debug(f"Risk Param based Key (:{key}")

            rec = self.risk_params.get(key, {'reward_factor': self.default_reward_factor,
                                             'risk_reward_ratio': self.default_risk_reward_ratio})

            logger.debug(f"Rec:{rec}")

            reward_factor = rec.get('reward_factor', self.default_reward_factor)
            risk_reward_ratio = rec.get('risk_reward_ratio', self.default_risk_reward_ratio)
            trail_sl_factor = rec.get('trail_sl_factor', self.default_trail_sl_factor)

        logger.debug(f"Params: {reward_factor}, {risk_reward_ratio}, {trail_sl_factor}")

        target_range = abs(pred_target - entry)

        adj_target_range = target_range * (1 + reward_factor)

        adj_sl_range = adj_target_range * risk_reward_ratio
        adj_trail_sl_range = adj_sl_range * trail_sl_factor

        ret_adj_target_range = round_price(price=adj_target_range, tick=tick, scrip=scrip)
        ret_adj_sl_range = round_price(price=adj_sl_range, tick=tick, scrip=scrip)
        ret_adj_trail_sl_range = round_price(price=adj_trail_sl_range, tick=tick, scrip=scrip)
        logger.debug(f"Target:{ret_adj_target_range} SL:{ret_adj_sl_range} Trail SL: {ret_adj_trail_sl_range}")
        return ret_adj_target_range, ret_adj_sl_range, ret_adj_trail_sl_range


if __name__ == '__main__':
    from commons.loggers.setup_logger import setup_logging

    setup_logging("risk-calc.log")
    accu_df_ = pd.read_csv("/Users/pralhad/Documents/99-src/98-trading/TechnoPunter-Commons/"
                           "resources/test/risk-calc/Portfolio-Accuracy.csv")
    rc = RiskCalc(accu_df_)
    _scrip = "X"
    _strategy = "Y"
    _signal = 1
    _tick = 0.05
    _acct = "X"
    _entry = 100.00
    _pred_target = 101.00
    t_range, s_range, t_sl_range = rc.calc_risk_params(scrip=_scrip, strategy=_strategy, signal=_signal,
                                                       tick=_tick, acct=_acct,
                                                       entry=_entry, pred_target=_pred_target)
    print(f"{t_range}, {s_range}, {t_sl_range}")

    _scrip = "NSE_APOLLOHOSP"
    _strategy = "trainer.strategies.rfcV2"
    _trade_dt = '2023-10-05'
    _signal = 1

    t_range, s_range, t_sl_range = rc.calc_risk_params(scrip=_scrip, strategy=_strategy, signal=_signal, tick=_tick,
                                                       acct=_acct,
                                                       entry=_entry, pred_target=_pred_target, risk_date=_trade_dt)
    print(f"{t_range}, {s_range}, {t_sl_range}")
