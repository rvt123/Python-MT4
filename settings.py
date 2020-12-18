## DATA DOWNLOADER SETTINGS
MODE = "LIVE"
# MODE = "BACKTEST"
if MODE == "LIVE":
    TIMEPERIOD = ['60']
    DELAY_DICT = {60:2}
    ENABLE_INTERVAL_TIME_DICT = {'60':2,'RUN':{'60':{'HOUR':16,'MINUTE':37}}}
    DISABLE_INTERVAL_TIME_DICT = {'60':{'HOUR':15,'MINUTE':20}}

elif MODE == "BACKTEST":
    TIMEPERIOD = ['15','30','60']
    DELAY_DICT = {5:1,15:1,30:3,60:5}
    ENABLE_INTERVAL_TIME_DICT = {'15':2,'30':5,'60':8,'RUN':{'15':{'HOUR':16,'MINUTE':33},'30':{'HOUR':16,'MINUTE':36},'60':{'HOUR':16,'MINUTE':39}}}
    DISABLE_INTERVAL_TIME_DICT = {'15':{'HOUR':15,'MINUTE':27},'30':{'HOUR':15,'MINUTE':28},'60':{'HOUR':15,'MINUTE':29}}

TESTING_TIMEPERIOD = ['60']
GENERATE_DATA_FILE_TIMEPERIOD = ['15','30','60']

DATA_DIRECTORY = "C://Users//raghu//Desktop//Create_data//NSE_DATA//"
UPDATE_CSV_PATH = "C://Users//raghu//Desktop//Create_data//"
# HOLIDAY_CSV_PATH = "C://Users//raghu//Desktop//Create_data//HolidaycalenderData.csv"
HOLIDAY_CSV_PATH = "C://Users//raghu//Desktop//Create_data//Holiday.csv"
DATA_DOWNLOADER_DEBUG = False
SAVE_SIGNAL_DICT = {'15':True,'30':True,'60':True}
# TELEGRAM_NOTIFY_DICT  = {'15':False,'30':True,'60':True}
TELEGRAM_NOTIFY_DICT  = {'15':False,'30':False,'60':False}
## History data
HISTORY_FILE_PATH = ["C://Users//raghu//AppData//Roaming//MetaQuotes//Terminal//50CA3DFB510CC5A8F28B48D1BF2A5702//history//Tradize-Demo//",\
               "C://Users//raghu//AppData//Roaming//MetaQuotes//Terminal//50CA3DFB510CC5A8F28B48D1BF2A5702//history//SimpleFintech-Demo//"]
HISTORY_VERBOSE = False
HISTORY_BAR = 300
# HISTORY_BAR = -1 To create long data make it after updating history through mt4 update hist EA

## Date Generation Paramters
START_TIME_HOUR = 8
START_TIME_MINUTE = 45
END_TIME_HOUR = 14
END_TIME_MINUTE = 45
## Data Server settings
PUSH_PORT = 32768
PULL_PORT = 32769
PUSH_PULL_PORT = [[32768,32769],[32770,32771],[32772,32773],\
                [32774,32775],[32776,32777],[32778,32779],\
                [32780,32781],[32782,32783],[32784,32785],\
                [32786,32787],[32788,32789],[32790,32791]]

## CREATE DATA DOWNLOAD SETTINGS
TRADE_SYMBOLS_ABS_PATH = "C://Users//raghu//Desktop//Create_data//"
