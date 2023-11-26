import logging
import os

import numpy as np
import pandas as pd

from commons.config.reader import cfg
from commons.consts.consts import IST
from commons.dataprovider.ScripData import ScripData
from commons.dataprovider.database import DatabaseEngine
from commons.utils.Misc import get_bod_epoch

MODEL_PREFIX = 'trainer.strategies.'
FINAL_DF_COLS = [
    'scrip', 'strategy', 'date', 'signal', 'time', 'open', 'target', 'day_close', 'entry_price', 'target_candle',
    'max_mtm', 'target_pnl'
]
logger = logging.getLogger(__name__)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)


def get_target_pnl(row):
    if pd.isnull(row['target_candle']):
        return 0
    else:
        return abs(row['open'] - row['target'])


def get_eod_pnl(row):
    if row['target_pnl'] != 0:
        return row['target_pnl']
    elif row['signal'] == 1:
        return row['day_close'] - row['open']
    else:
        return row['open'] - row['day_close']


def is_valid(row):
    if (row['signal'] == 1 and row['target'] > row['open']) or \
            (row['signal'] == -1 and row['target'] < row['open']):
        return True
    else:
        return False


def target_met(row):
    if (row['curr_signal'] == 1 and row['curr_target'] <= row['high']) or \
            (row['curr_signal'] == -1 and row['curr_target'] >= row['low']):
        return True
    else:
        return False


def calc_mtm(row):
    if row['curr_signal'] == 1:
        return row['high'] - row['entry_price']
    elif row['curr_signal'] == -1:
        return row['entry_price'] - row['low']


class FastBT:
    trader_db: DatabaseEngine
    sd: ScripData

    def __init__(self, trader_db: DatabaseEngine = None):
        if trader_db is None:
            self.trader_db = DatabaseEngine()
        else:
            self.trader_db = trader_db
        self.sd = ScripData(trader_db=trader_db)

    def prep_data(self, scrip, strategy, raw_pred_df: pd.DataFrame):
        logger.info(f"Entering Prep data for {scrip} with {len(raw_pred_df)} predictions")

        if len(raw_pred_df) > 1:
            # This would be a backtest prediction file
            raw_pred_df = raw_pred_df[['target', 'signal', 'time']]
            raw_pred_df['time'] = raw_pred_df['time'].shift(-1)
        else:
            # This would be Next Close file
            pass

        raw_pred_df['date'] = pd.to_datetime(raw_pred_df['time'], unit='s', utc=True)
        raw_pred_df['date'] = raw_pred_df['date'].dt.tz_convert(IST)
        raw_pred_df['date'] = raw_pred_df['date'].dt.date
        # Remove last row after offset of time
        raw_pred_df.dropna(subset=['date'], inplace=True)

        # Get 1-Min data (tick data) and join with raw_pred_df
        start_date = raw_pred_df.date.min()
        tick_data = self.sd.get_tick_data(scrip, from_date=start_date)
        merged_df = pd.merge(tick_data, raw_pred_df, how='left', left_on='time', right_on='time')
        merged_df['datetime'] = pd.to_datetime(merged_df['time'], unit='s', utc=True)
        merged_df['datetime'] = merged_df['datetime'].dt.tz_convert(IST)
        merged_df['date'] = merged_df['datetime'].dt.date
        merged_df.set_index('datetime', inplace=True)

        # Get the Daily data (base data) and join with merged DF this is to get the closing price for the day
        base_data = self.sd.get_base_data(scrip, from_date=start_date)
        base_data = base_data[['time', 'close']]
        base_data.rename(columns={"close": "day_close"}, inplace=True)
        merged_df = pd.merge(merged_df, base_data, how='left', left_on='time', right_on='time')
        # Remove 1st Row since we don't have closing from T-1
        merged_df = merged_df.iloc[1:]
        return merged_df, len(raw_pred_df)

    def calc_stats(self, final_df, count, scrip, strategy):

        l_trades = 0
        l_pct_success = 0
        l_pnl = 0
        l_avg_cost = 0.01
        s_trades = 0
        s_pct_success = 0
        s_pnl = 0
        s_avg_cost = 0.01
        if len(final_df) > 0:
            pct_success = (final_df['target_candle'].notna().sum() / len(final_df)) * 100
            tot_pnl = final_df['target_pnl'].sum()
            logger.debug(f"For {scrip} using {strategy}: No. of trades: {len(final_df)} "
                         f"with {format(pct_success, '.2f')}% Accuracy "
                         f"& PNL {format(tot_pnl, '.2f')}")
            l_trades = final_df.loc[final_df.signal == 1]
            if len(l_trades) > 0:
                l_pct_success = (l_trades['target_candle'].notna().sum() / len(l_trades)) * 100
                l_avg_cost = l_trades['entry_price'].mean()
                l_pnl = l_trades['target_pnl'].sum()
                logger.debug(f"For {scrip} using {strategy}: Long: No. of trades: {len(l_trades)} "
                             f"with {format(l_pct_success, '.2f')}% Accuracy "
                             f"& PNL {format(l_pnl, '.2f')}")
            s_trades = final_df.loc[final_df.signal == -1]
            if len(s_trades) > 0:
                s_pct_success = (s_trades['target_candle'].notna().sum() / len(s_trades)) * 100
                s_pnl = s_trades['target_pnl'].sum()
                s_avg_cost = s_trades['entry_price'].mean()
                logger.debug(f"For {scrip} using {strategy}: Short: No. of trades: {len(s_trades)} "
                             f"with {format(s_pct_success, '.2f')}% Accuracy "
                             f"& PNL {format(s_pnl, '.2f')}")
        return {
            "scrip": scrip, "strategy": MODEL_PREFIX + strategy,
            "trades": len(final_df), "entry_pct": len(final_df) * 100 / count,
            "l_trades": len(l_trades), "l_pct_success": l_pct_success, "l_pnl": l_pnl, "l_avg_cost": l_avg_cost,
            "l_pct": l_pnl * 100 / l_avg_cost, "l_entry_pct": len(l_trades) * 100 / count,
            "s_trades": len(s_trades), "s_pct_success": s_pct_success, "s_pnl": s_pnl, "s_avg_cost": s_avg_cost,
            "s_pct": s_pnl * 100 / s_avg_cost, "s_entry_pct": len(s_trades) * 100 / count
        }

    def get_accuracy(self, param):

        setup_logging()
        func_logger = logging.getLogger(__name__)

        if isinstance(param, dict):
            strategy = param.get('strategy')
            scrip = param.get('scrip')
            raw_pred_df = param.get('raw_pred_df')
            func_logger.debug(f"Getting dict based results for {scrip} & {strategy}")
            merged_df, count = self.prep_data(scrip, strategy, raw_pred_df=raw_pred_df)
        else:
            _, rec = param
            df = pd.DataFrame([rec])
            strategy = rec.get('model')
            scrip = rec.get('scrip')
            func_logger.debug(f"Getting DF based results for {scrip} & {strategy}")
            trade_time = get_bod_epoch(rec.get('trade_date'))
            df.loc[:, 'time'] = trade_time
            merged_df, count = self.prep_data(scrip, strategy, raw_pred_df=df[['target', 'signal', 'time']])

        # Is the target still available at open i.e. 9:15 candle?
        merged_df['is_valid'] = merged_df.apply(is_valid, axis=1)

        # If Yes - use that as entry price for the entire day
        merged_df['entry_price'] = merged_df['open'][merged_df['is_valid']]

        # Identify valid days
        valid_df = merged_df.loc[merged_df.is_valid]
        if len(valid_df) == 0:
            return pd.DataFrame(), {}
        valid_df.set_index('date', inplace=True)
        merged_df = merged_df[merged_df.date.isin(valid_df.index)]

        # Fill the data for the day
        merged_df['entry_price'] = merged_df['entry_price'].ffill()
        merged_df['curr_target'] = merged_df['target'].ffill()
        merged_df['curr_signal'] = merged_df['signal'].ffill()

        # Check for signals & events i.e.
        # Target Met - 1st row
        # SL Hit
        # Max Profit : MTM + Max
        # Max Loss

        merged_df['target_met'] = merged_df.apply(target_met, axis=1)
        if len(merged_df[merged_df['target_met']]) > 0:
            target_met_df = merged_df[merged_df['target_met']].groupby('date').apply(lambda r: r['target_met'].idxmin())
            final_df = pd.merge(valid_df, pd.DataFrame(target_met_df), how='left', left_index=True, right_index=True)
            final_df.rename({0: 'target_candle'}, axis=1, inplace=True)
        else:
            final_df = valid_df
            final_df.loc[:, 'target_candle'] = float('nan')

        final_df.reset_index(inplace=True)
        merged_df['mtm'] = merged_df.apply(calc_mtm, axis=1)
        mtm_max_df = merged_df.groupby('date').apply(lambda r: r['mtm'].max())
        final_df = pd.merge(final_df, pd.DataFrame(mtm_max_df), how='left', left_index=True, right_index=True)
        final_df.rename({0: 'max_mtm'}, axis=1, inplace=True)

        # Calc PNL
        # Target PNL
        # EOD PNL - Could be P or L
        final_df['target_pnl'] = final_df.apply(get_target_pnl, axis=1)
        final_df['target_pnl'] = final_df.apply(get_eod_pnl, axis=1)

        # Prep to write to CSV
        final_df['strategy'] = MODEL_PREFIX + strategy
        final_df['scrip'] = scrip
        final_df.drop(columns=['high', 'low', 'close'], axis=1, inplace=True)

        cols = ["open", "day_close", "target", "entry_price", "max_mtm", "target_pnl"]
        final_df[cols] = final_df[cols].astype(float).apply(lambda x: np.round(x, decimals=2))

        stats = self.calc_stats(final_df, count, scrip, strategy)
        func_logger.debug(f"Evaluated: {scrip} & {strategy} with {len(final_df)} trades")
        return final_df[FINAL_DF_COLS], stats

    def run_accuracy(self, params: list[dict]):
        logger.info(f"run_accuracy: Started with {len(params)} scrips")
        trades = []
        stats = []
        for param in params:
            trade, stat = self.get_accuracy(param)
            trades.append(trade)
            stats.append(stat)
        # try:
        #     pool = Pool(processes=8)
        #     result_set = pool.imap(self.get_accuracy, params)
        #     for trade, stat in result_set:
        #         trades.append(trade)
        #         stats.append(stat)
        # except Exception as ex:
        #     logger.error(f"Error in Multi Processing {ex}")
        result_trades = pd.concat(trades)
        result_trades.sort_values(by=['date', 'scrip'], inplace=True)
        result_stats = pd.DataFrame(stats)
        return result_trades, result_stats

    def run_cob_accuracy(self, params: pd.DataFrame):
        logger.info(f"run_accuracy: Started with {len(params)} scrips")
        trades = []
        stats = []
        valid_trades = params.loc[params.entry_order_status == 'COMPLETE']
        for param in valid_trades.iterrows():
            trade, stat = self.get_accuracy(param)
            trades.append(trade)
            stats.append(stat)
        # try:
        #     pool = Pool()
        #     result_set = pool.imap(self.get_accuracy, valid_trades.iterrows())
        #     for trade, stat in result_set:
        #         trades.append(trade)
        #         stats.append(stat)
        # except Exception as ex:
        #     logger.error(f"Error in Multi Processing {ex}")
        result_trades = pd.concat(trades)
        result_trades.sort_values(by=['date', 'scrip'], inplace=True)
        result_stats = pd.DataFrame(stats)
        result_stats.dropna(subset=['scrip'], inplace=True)
        return result_trades, result_stats


if __name__ == '__main__':
    from commons.loggers.setup_logger import setup_logging

    setup_logging("fastBT.log")

    f = FastBT()
    params_ = []
    for scrip_ in cfg['steps']['scrips']:
        for strategy_ in cfg['steps']['strats']:
            file = os.path.join(cfg['generated'], scrip_, f'trainer.strategies.{strategy_}.{scrip_}_Raw_Pred.csv')
            raw_pred_df_ = pd.read_csv(file)
            params_.append({"scrip": scrip_, "strategy": strategy_, "raw_pred_df": raw_pred_df_})

    bt_trades, bt_stats = f.run_accuracy(params_)
    logger.info(f"\n{bt_trades}")
    logger.info(f"\n{bt_stats}")

    params_df = pd.read_json(
        "/Users/pralhad/Documents/99-src/98-trading/trade-exec-engine/resources/test/cob/cob-params.json")
    params_df.loc[:, 'trade_date'] = '2023-11-24'
    bt_trades, bt_stats = f.run_cob_accuracy(params=params_df)
    logger.info(f"\n{bt_trades}")
    logger.info(f"\n{bt_stats}")
