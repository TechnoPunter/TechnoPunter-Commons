import datetime
import enum
import os

import pytz

from commons.config.reader import cfg as _cfg

# Paths
GENERATED_DATA_PATH = _cfg['generated']
MODELS_PATH = GENERATED_DATA_PATH + "models/"

# File Name Patterns
PRED_FILE_NAME = "_Predict.csv"
RESULT_FILE_NAME = "_Result.csv"
RAW_PRED_FILE_NAME = "_Raw_Pred.csv"
NEXT_CLOSE_FILE_NAME = "_Next_Close.csv"
PNL_FILE_NAME = "_PNL.csv"
MODEL_SCRIPT_NAME = '_tpot_exported_pipeline.py'
MODEL_SAVE_FILE_NAME = '.sav'
COLUMN_SEPARATOR = ","

# Time Related
IST = pytz.timezone('Asia/Kolkata')
TODAY = datetime.datetime.today().date()
S_TODAY = str(TODAY)

# Log Types
PARAMS_LOG_TYPE = "Params"
BROKER_TRADE_LOG_TYPE = "BrokerTrades"
BT_TRADE_LOG_TYPE = "BacktestTrades"

# Models
LOG_STORE_MODEL = "LogStore"
SCRIP_HIST = "ScripHist"
TRADES_MTM_TABLE = "TradesMTM"

# Trainer Paths
SUMMARY_PATH = os.path.join(_cfg['generated'], 'summary')
ACCURACY_FILE = os.path.join(SUMMARY_PATH, 'Portfolio-Accuracy.csv')
TRADES_FILE = os.path.join(SUMMARY_PATH, 'Portfolio-Trades.csv')
TRADES_MTM_FILE = os.path.join(SUMMARY_PATH, 'Portfolio-Trades-MTM.csv')


class Interval(enum.Enum):
    in_1_minute = "1"
    in_3_minute = "3"
    in_5_minute = "5"
    in_15_minute = "15"
    in_30_minute = "30"
    in_45_minute = "45"
    in_1_hour = "1H"
    in_2_hour = "2H"
    in_3_hour = "3H"
    in_4_hour = "4H"
    in_daily = "1D"
    in_weekly = "1W"
    in_monthly = "1M"
