import logging
from multiprocessing import Pool

import pandas as pd

from commons.backtest.getBTResult import get_bt_result
from commons.config.reader import cfg
from commons.consts.consts import *
from commons.dataprovider.ScripData import ScripData
from commons.service.RiskCalc import RiskCalc
from commons.utils.Misc import get_bod_epoch

logger = logging.getLogger(__name__)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.options.mode.chained_assignment = None

MODE = "SERVER"


class FastBT:
    rc: RiskCalc

    def __init__(self, exec_mode: str = MODE, risk_mode: str = "PRESET", accuracy_df: pd.DataFrame = None,
                 scrip_data: ScripData = None):
        self.mode = "BACKTEST"  # "NEXT-CLOSE"
        self.exec_mode = exec_mode
        self.rc = RiskCalc(mode=risk_mode, accuracy=accuracy_df)
        if scrip_data is None:
            self.sd = ScripData()
        else:
            self.sd = scrip_data

    def prep_data(self, scrip, strategy, raw_pred_df: pd.DataFrame, sd: ScripData):
        logger.info(f"Entering Prep data for {scrip} with {len(raw_pred_df)} predictions")

        # Need date for fetch data and readability
        raw_pred_df['date'] = pd.to_datetime(raw_pred_df['time'], unit='s', utc=True)
        raw_pred_df['date'] = raw_pred_df['date'].dt.tz_convert(IST)
        raw_pred_df['date'] = raw_pred_df['date'].dt.date
        start_date = raw_pred_df.date.min()

        # Get the 1-min data
        tick_data = sd.get_tick_data(scrip, from_date=start_date)

        # Get the Daily data (base data)
        if self.mode == "BACKTEST":
            base_data = sd.get_base_data(scrip, from_date=start_date)
        else:
            # For NEXT-CLOSE we won't have base date at COB
            # Last candle of the day is closing price for the day! However, time of day is 1st candle's epoch
            base_data = tick_data.iloc[-1:]
            base_data.loc[:, 'time'] = tick_data.iloc[0]['time']
        base_data = base_data[['time', 'close']]
        base_data.rename(columns={"close": "day_close"}, inplace=True)

        # Get Previous day close populated to allow for bol_signal_strength calc
        raw_pred_df = pd.merge(raw_pred_df, base_data, how="left", left_on='time', right_on='time')
        raw_pred_df.rename(columns={"day_close": "prev_day_close"}, inplace=True)

        if len(raw_pred_df) > 1:
            # This would be a backtest prediction file
            raw_pred_df = raw_pred_df[['target', 'signal', 'time', 'date', 'prev_day_close']]
            # Since the prediction happens for next working day - need to shift the time up by 1 row
            raw_pred_df['time'] = raw_pred_df['time'].shift(-1)
        else:
            # This would be Next Close file
            pass

        raw_pred_df.rename(columns={"target": "pred_target"}, inplace=True)
        # Remove last row after offset of time
        raw_pred_df.dropna(subset=['date'], inplace=True)

        # Join tick data with raw_pred_df
        merged_df = pd.merge(tick_data, raw_pred_df, how='left', left_on='time', right_on='time')
        merged_df['datetime'] = pd.to_datetime(merged_df['time'], unit='s', utc=True)
        merged_df['datetime'] = merged_df['datetime'].dt.tz_convert(IST)
        merged_df['date'] = merged_df['datetime'].dt.date
        merged_df.loc[merged_df.groupby(merged_df.date).apply(lambda x: x.index[-1]), 'cob_row'] = 1.0
        merged_df['cob_row'] = merged_df['cob_row'].fillna(0.0)
        # merged_df.set_index('datetime', inplace=True)

        # Join base data with raw_pred_df for getting day close
        merged_df = pd.merge(merged_df, base_data, how='left', left_on='time', right_on='time')

        if self.mode == "BACKTEST":
            # Remove 1st Row since we don't have closing from T-1
            merged_df = merged_df.iloc[1:]

        merged_df.loc[:, 'scrip'] = scrip
        merged_df.loc[:, 'strategy'] = strategy
        merged_df.loc[:, 'tick'] = 0.05

        return merged_df

    def run_accuracy(self, params: list[dict]):
        logger.info(f"run_accuracy: Started with {len(params)} scrips")
        self.mode = "BACKTEST"

        if len(params) == 0:
            logger.error(f"Unable to proceed since Params is empty")
            return
        trades = []
        stats = []
        mtm = {}
        accuracy_params = []
        for param in params:
            strategy = param.get('strategy')
            scrip = param.get('scrip')
            raw_pred_df = param.get('raw_pred_df')
            logger.info(f"Getting dict based results for {scrip} & {strategy}")
            merged_df = self.prep_data(scrip, strategy, raw_pred_df=raw_pred_df, sd=self.sd)
            accuracy_params.append({"scrip": scrip, "strategy": strategy, "merged_df": merged_df, "risk_calc": self.rc})
        if self.exec_mode == "SERVER":
            try:
                logger.info(f"About to start accuracy calc with {len(accuracy_params)} objects")
                with Pool() as pool:
                    for result in pool.imap(get_bt_result, accuracy_params):
                        key, trade, stat, mtm_df = result
                        trades.append(trade)
                        stats.append(stat)
                        mtm[key] = mtm_df

            except Exception as ex:
                logger.error(f"Error in Multi Processing {ex}")
        else:
            for accu_param in accuracy_params:
                key, trade, stat, mtm_df = get_bt_result(accu_param)
                trades.append(trade)
                stats.append(stat)
                mtm[key] = mtm_df
        result_trades = pd.concat(trades)
        result_trades.sort_values(by=['date', 'scrip'], inplace=True)
        result_stats = pd.concat(stats)
        return result_trades, result_stats, mtm

    def run_cob_accuracy(self, params: pd.DataFrame):
        logger.info(f"run_accuracy: Started with {len(params)} scrips")
        self.mode = "NEXT-CLOSE"

        if len(params) == 0:
            logger.error(f"Unable to proceed since Params is empty")
            return
        trades = []
        stats = []
        mtm = {}
        accuracy_params = []
        valid_trades = params.loc[params.entry_order_status == 'ENTERED']
        logger.info(f"No. of valid trades: {len(valid_trades)}")
        for param in valid_trades.iterrows():
            _, rec = param
            df = pd.DataFrame([rec])
            strategy = rec.get('model')
            scrip = rec.get('scrip')
            logger.info(f"Getting DF based results for {scrip} & {strategy}")
            trade_date = datetime.datetime.fromtimestamp(int(rec.get('entry_ts')))
            trade_time = get_bod_epoch(trade_date.strftime('%Y-%m-%d'))
            df.loc[:, 'time'] = trade_time
            merged_df = self.prep_data(scrip, strategy, raw_pred_df=df[['target', 'signal', 'time']], sd=self.sd)
            accuracy_params.append({"scrip": scrip, "strategy": strategy, "merged_df": merged_df, "risk_calc": self.rc})
        if self.mode == "SERVER":
            try:
                with Pool() as pool:
                    for result in pool.imap(get_bt_result, accuracy_params):
                        key, trade, stat, mtm_df = result
                        trades.append(trade)
                        stats.append(stat)
                        mtm[key] = mtm_df
            except Exception as ex:
                logger.error(f"Error in Multi Processing {ex}")
        else:
            for accu_param in accuracy_params:
                key, trade, stat, mtm_df = get_bt_result(accu_param)
                trades.append(trade)
                stats.append(stat)
                mtm[key] = mtm_df
        if len(trades) > 0:
            result_trades = pd.concat(trades)
            result_trades.sort_values(by=['date', 'scrip'], inplace=True)
            result_stats = pd.concat(stats)
            result_stats.dropna(subset=['scrip'], inplace=True)
        else:
            result_trades = pd.DataFrame()
            result_stats = pd.DataFrame()
        return result_trades, result_stats, mtm


if __name__ == '__main__':
    from commons.loggers.setup_logger import setup_logging

    setup_logging("fastBT.log")

    f = FastBT(exec_mode="LOCAL")
    params_ = []
    for scrip_ in cfg['steps']['scrips']:
        for strategy_ in cfg['steps']['strats']:
            file = str(os.path.join(cfg['generated'], scrip_, f'trainer.strategies.{strategy_}.{scrip_}_Raw_Pred.csv'))
            raw_pred_df_ = pd.read_csv(file)
            params_.append({"scrip": scrip_, "strategy": strategy_, "raw_pred_df": raw_pred_df_})

    bt_trades, bt_stats, bt_mtm = f.run_accuracy(params_)
    logger.info(f"bt_trades#: {len(bt_trades)}")
    logger.debug(f"bt_trades:\n{bt_trades}")
    logger.info(f"bt_stats#:{len(bt_stats)}")
    logger.debug(f"bt_stats:\n{bt_stats}")
    logger.info(f"bt_mtm#: {len(bt_mtm)}")

    # exit

    from commons.service.LogService import LogService
    from commons.dataprovider.database import DatabaseEngine

    acct_ = 'Trader-V2-Pralhad'
    dt_ = '2023-12-01'
    db = DatabaseEngine()
    ls = LogService(db)
    data = ls.get_log_entry_data(log_type=PARAMS_LOG_TYPE, keys=["COB"], log_date=dt_, acct=acct_)
    bt_trades, bt_stats, bt_mtm = f.run_cob_accuracy(params=pd.DataFrame(data))
    logger.info(f"bt_trades#: {len(bt_trades)}")
    logger.debug(f"bt_trades:\n{bt_trades}")
    logger.info(f"bt_stats#:{len(bt_stats)}")
    logger.debug(f"bt_stats:\n{bt_stats}")
    logger.info(f"bt_mtm#: {len(bt_mtm)}")
