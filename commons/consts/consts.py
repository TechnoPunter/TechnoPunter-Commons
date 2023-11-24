import datetime

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

# Models
LOG_STORE_MODEL = "LogStore"
