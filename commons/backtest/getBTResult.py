import logging

import pandas as pd

from commons.service.RiskCalc import RiskCalc
from commons.utils.Misc import remove_outliers, get_updated_sl

logger = logging.getLogger(__name__)

TRADE_DF_COLS = {
    'scrip': pd.Series(dtype='str'),
    'strategy': pd.Series(dtype='str'),
    'tick': pd.Series(dtype='float'),
    'date': pd.Series(dtype='str'),
    'signal': pd.Series(dtype='int'),
    'target': pd.Series(dtype='float'),
    'bod_strength': pd.Series(dtype='float'),
    'bod_sl': pd.Series(dtype='float'),
    'sl_range': pd.Series(dtype='float'),
    'trail_sl': pd.Series(dtype='float'),
    'strength': pd.Series(dtype='float'),
    'entry_time': pd.Series(dtype='int'),
    'entry_price': pd.Series(dtype='float'),
    'status': pd.Series(dtype='str'),
    'exit_price': pd.Series(dtype='float'),
    'exit_time': pd.Series(dtype='int'),
    'pnl': pd.Series(dtype='float'),
    'sl': pd.Series(dtype='float'),
    'sl_update_cnt': pd.Series(dtype='int'),
    'max_mtm': pd.Series(dtype='float'),
    'max_mtm_pct': pd.Series(dtype='float'),
}

MTM_DF_COLS = [
    'scrip', 'strategy', 'trade_date', 'datetime', 'signal', 'time', 'open', 'high', 'low', 'close',
    'target', 'target_met', 'entry_price', 'mtm', 'mtm_pct'
]


def get_pnl(row):
    if row.signal == 1:
        return round((row.exit_price - row.entry_price), 2)
    else:
        return round((row.entry_price - row.exit_price), 2)


def get_strength(row):
    if row.signal == 1:
        return row.target - row.entry_price
    else:
        return row.entry_price - row.target


def is_valid(row):
    if pd.isnull(row['signal']):
        return float('nan')
    elif (row['signal'] == 1 and row['pred_target'] > row['open']) or \
            (row['signal'] == -1 and row['pred_target'] < row['open']):
        return True
    else:
        return False


def target_met(row):
    if (row['curr_signal'] == 1 and row['curr_target'] <= row['high']) or \
            (row['curr_signal'] == -1 and row['curr_target'] >= row['low']):
        return True
    else:
        return False


def sl_hit(row):
    if (row['curr_signal'] == 1 and row['curr_sl'] >= row['low']) or \
            (row['curr_signal'] == -1 and row['curr_sl'] <= row['high']):
        return True
    else:
        return False


def calc_mtm(row, low=None, high=None):
    if row['signal'] == 1:
        mtm = high - row['entry_price']
        mtm_pct = mtm * 100 / row['entry_price']
        return round(mtm, 2), round(mtm_pct, 2)
    elif row['signal'] == -1:
        mtm = row['entry_price'] - low
        mtm_pct = mtm * 100 / row['entry_price']
        return round(mtm, 2), round(mtm_pct, 2)
    else:
        return 0.0, 0.0


def calc_mtm_df(row):
    low = row['low']
    high = row['high']
    if row.get('curr_signal') == 1:
        mtm = high - row['entry_price']
        mtm_pct = mtm * 100 / row['entry_price']
        return round(mtm, 2), round(mtm_pct, 2)
    elif row.get('curr_signal') == -1:
        mtm = row['entry_price'] - low
        mtm_pct = mtm * 100 / row['entry_price']
        return round(mtm, 2), round(mtm_pct, 2)
    else:
        return 0.0, 0.0


def enrich_risk(df_to_enrich: pd.DataFrame, risk_calc: RiskCalc, acct: str = 'Trader-V2-Pralhad') -> pd.DataFrame:
    result = df_to_enrich.copy()
    for idx, rec in result.loc[pd.notnull(result.signal)].iterrows():
        t_r, sl_r, t_sl_r = risk_calc.calc_risk_params(scrip=rec.scrip, strategy=rec.strategy, signal=rec.signal,
                                                       tick=0.05, acct=acct,
                                                       prev_close=rec.prev_day_close, entry=rec.open,
                                                       pred_target=rec.pred_target, risk_date=str(rec.date))
        result.loc[idx, ['target_range', 'sl_range', 'trail_sl']] = float(t_r), float(sl_r), float(t_sl_r)

    return result


def calc_stats(input_df, scrip, strategy):
    results = []
    for trade_dt in input_df.date.unique():
        logger.info(f"About to process for trade_dt: {trade_dt}")
        final_df = input_df.loc[input_df.date <= trade_dt]
        l_num_predictions = 0
        l_pct_success = 0
        l_valid_count = 0
        l_pct_entry = 0.0
        l_pnl = 0
        l_avg_cost = 0.01
        l_pct_returns = 0.0
        l_reward_factor = 0
        s_num_predictions = 0
        s_pct_success = 0
        s_valid_count = 0
        s_pct_entry = 0.0
        s_pnl = 0
        s_avg_cost = 0.01
        s_pct_returns = 0.0
        s_reward_factor = 0
        count = len(final_df)
        if len(final_df) > 0:
            l_trades = final_df.loc[final_df.signal == 1]
            if len(l_trades) > 0:
                l_num_predictions = len(l_trades)
                l_valid_count = len(l_trades.loc[l_trades.status != 'INVALID'])
                if l_valid_count == 0:
                    l_pct_entry = 0.0
                    l_pct_success = 0.0
                    l_avg_cost = 0.0
                    l_pct_returns = 0.0
                else:
                    l_mtm_records = remove_outliers(l_trades['max_mtm'])
                    if len(l_mtm_records) > 0:
                        l_reward_factor = round(
                            (l_mtm_records.sum() / l_trades['bod_strength'].loc[l_mtm_records.index].sum()) - 1, 2)
                    l_pct_entry = (l_valid_count / l_num_predictions) * 100
                    l_pct_entry = round(l_pct_entry, 2)
                    l_success = l_trades.loc[l_trades.status == 'TARGET-HIT']
                    l_pct_success = (len(l_success) / l_valid_count) * 100
                    l_pct_success = round(l_pct_success, 2)
                    l_avg_cost = l_trades['entry_price'].mean()
                    l_pnl = round(l_trades['pnl'].sum(), 2)
                    l_pct_returns = round((l_pnl * 100 / l_avg_cost), 2)

            s_trades = final_df.loc[final_df.signal == -1]
            if len(s_trades) > 0:
                s_num_predictions = len(s_trades)
                s_valid_count = len(s_trades.loc[s_trades.status != 'INVALID'])
                if s_valid_count == 0:
                    s_pct_entry = 0.0
                    s_pct_success = 0.0
                    s_avg_cost = 0.0
                    s_pct_returns = 0.0
                else:
                    s_mtm_records = remove_outliers(s_trades['max_mtm'])
                    if len(s_mtm_records) > 0:
                        s_reward_factor = round(
                            (s_mtm_records.sum() / s_trades['bod_strength'].loc[s_mtm_records.index].sum()) - 1, 2)
                    s_pct_entry = (s_valid_count / s_num_predictions) * 100
                    s_pct_entry = round(s_pct_entry, 2)
                    s_success = s_trades.loc[s_trades.status == 'TARGET-HIT']
                    s_pct_success = (len(s_success) / s_valid_count) * 100
                    s_pct_success = round(s_pct_success, 2)
                    s_avg_cost = s_trades['entry_price'].mean()
                    s_pnl = round(s_trades['pnl'].sum(), 2)
                    s_pct_returns = round((s_pnl * 100 / s_avg_cost), 2)
        results.append({
            "scrip": scrip, "strategy": strategy, "trade_date": trade_dt,
            "trades": count, "pct_entry": round(((l_valid_count + s_valid_count) * 100 / count), 2),
            "l_num_predictions": l_num_predictions, "l_num_trades": l_valid_count,
            "l_pct_success": l_pct_success, "l_pnl": l_pnl, "l_avg_cost": l_avg_cost,
            "l_pct_returns": l_pct_returns, "l_pct_entry": l_pct_entry,
            "l_reward_factor": l_reward_factor,
            "s_num_predictions": s_num_predictions, "s_num_trades": s_valid_count,
            "s_pct_success": s_pct_success, "s_pnl": s_pnl, "s_avg_cost": s_avg_cost,
            "s_pct_returns": s_pct_returns, "s_pct_entry": s_pct_entry,
            "s_reward_factor": s_reward_factor,
        })
    return pd.DataFrame(results)


def get_bt_result(accu_params):
    scrip = accu_params.get('scrip')
    strategy = accu_params.get('strategy')
    merged_df = accu_params.get('merged_df')
    risk_calc = accu_params.get('risk_calc')
    # TODO: Logging
    logger.info(f"Starting get_accuracy for: {scrip} & {strategy}")
    print(f"Starting get_accuracy for: {scrip} & {strategy}")

    # Adjust the target as per Risk Calc.
    merged_df = enrich_risk(merged_df, risk_calc=risk_calc)

    # Calc Target & SL now
    merged_df['target'] = merged_df['open'] + merged_df['signal'] * merged_df['target_range']
    merged_df['sl'] = merged_df['open'] - merged_df['signal'] * merged_df['sl_range']

    # Is the target still available at open i.e. 9:15 candle? If so mark full day as is_valid.
    # Remove rows which couldn't be evaluated
    merged_df['is_valid'] = merged_df.apply(is_valid, axis=1)

    # If Yes - use that as entry price for the entire day
    merged_df['entry_price'] = merged_df['open'][merged_df['is_valid'] == True]

    # Fill the data for the day
    merged_df['is_valid'] = merged_df['is_valid'].ffill()
    merged_df.dropna(subset=['is_valid'], inplace=True)
    merged_df['entry_price'] = merged_df['entry_price'].ffill()
    merged_df['curr_signal'] = merged_df['signal'].ffill()
    merged_df['day_close'] = merged_df['day_close'].ffill()
    merged_df['curr_target'] = merged_df['target'].ffill()
    merged_df['bod_sl'] = merged_df['sl'].ffill()
    merged_df['curr_trail_sl'] = merged_df['trail_sl'].ffill()

    merged_df.drop(columns=["target_range", "sl_range"], inplace=True)

    trades = pd.DataFrame(TRADE_DF_COLS)

    mtm_df = merged_df.copy()
    mtm_df['target_met'] = mtm_df.apply(target_met, axis=1)
    mtm_df[['mtm', 'mtm_pct']] = mtm_df.apply(calc_mtm_df, axis=1, result_type='expand')
    mtm_df.rename(columns={"date": "trade_date"}, inplace=True)
    mtm_df.reset_index(inplace=True)

    trade_idx = -1
    for idx, rec in merged_df.iterrows():
        if not (pd.isna(rec.signal)):
            # Open a position
            trade_idx += 1
            trades.loc[trade_idx, 'scrip'] = scrip
            trades.loc[trade_idx, 'strategy'] = strategy
            trades.loc[trade_idx, 'tick'] = 0.05
            trades.loc[trade_idx, 'date'] = rec.date
            trades.loc[trade_idx, 'signal'] = rec.signal
            trades.loc[trade_idx, 'target'] = rec.target
            trades.loc[trade_idx, 'bod_sl'] = rec.bod_sl
            trades.loc[trade_idx, 'sl'] = rec.bod_sl
            trades.loc[trade_idx, 'sl_range'] = round(abs(rec.bod_sl - rec.open), 2)
            trades.loc[trade_idx, 'trail_sl'] = rec.trail_sl
            trades.loc[trade_idx, 'entry_price'] = rec.open
            trades.loc[trade_idx, 'entry_time'] = rec.time
            trades.loc[trade_idx, 'strength'] = get_strength(trades.iloc[trade_idx])
            trades.loc[trade_idx, 'bod_strength'] = round(abs(rec.prev_day_close - rec.target), 2)
            trades.loc[trade_idx, 'sl_update_cnt'] = 0
            trades.loc[trade_idx, 'max_mtm'] = 0.0
            trades.loc[trade_idx, 'max_mtm_pct'] = 0.0
            if rec.is_valid:
                trades.loc[trade_idx, 'status'] = 'OPEN'
            else:
                trades.loc[trade_idx, 'status'] = 'INVALID'
        # Housekeeping
        if trades.iloc[trade_idx]['status'] == 'OPEN':

            rec['curr_sl'] = trades.iloc[trade_idx]['sl']
            if sl_hit(rec):
                trades.loc[trade_idx, 'status'] = 'SL-HIT'
                trades.loc[trade_idx, 'exit_price'] = trades.iloc[trade_idx]['sl']
                trades.loc[trade_idx, 'exit_time'] = rec.time
                trades.loc[trade_idx, 'pnl'] = get_pnl(trades.loc[trade_idx])
            elif target_met(rec):
                trades.loc[trade_idx, 'status'] = 'TARGET-HIT'
                trades.loc[trade_idx, 'exit_price'] = rec.curr_target
                trades.loc[trade_idx, 'exit_time'] = rec.time
                trades.loc[trade_idx, 'pnl'] = get_pnl(trades.loc[trade_idx])

        if trades.iloc[trade_idx]['status'] == 'OPEN':
            # Still open i.e. Not SL or Target Hit
            new_sl = float(get_updated_sl(trades.iloc[trade_idx], rec.low, rec.high))
            logger.debug(f"New SL {new_sl} for rec: {rec} and trades:\n{trades}")
            if new_sl != 0.0:
                trades.loc[trade_idx, 'sl_update_cnt'] += 1
                trades.loc[trade_idx, 'sl'] = new_sl

        mtm, mtm_pct = calc_mtm(trades.loc[trade_idx], rec.low, rec.high)
        if mtm > trades.loc[trade_idx, 'max_mtm']:
            trades.loc[trade_idx, 'max_mtm'] = mtm
            trades.loc[trade_idx, 'max_mtm_pct'] = mtm_pct

        if trades.iloc[trade_idx]['status'] == 'OPEN' and rec.cob_row == 1.0:
            trades.loc[trade_idx, 'status'] = 'COB-CLOSE'
            trades.loc[trade_idx, 'exit_price'] = rec.close
            trades.loc[trade_idx, 'exit_time'] = rec.time
            trades.loc[trade_idx, 'pnl'] = get_pnl(trades.loc[trade_idx])

    stats = calc_stats(trades, scrip, strategy)
    logger.info(f"Evaluated: {scrip} & {strategy} with {len(trades)} trades")
    print(f"Evaluated: {scrip} & {strategy} with {len(trades)} trades")
    return f"{scrip}:{strategy}", trades, stats, mtm_df[MTM_DF_COLS]
