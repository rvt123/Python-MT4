from apscheduler.schedulers.blocking import BlockingScheduler
from DWX_MT4_UPDATE_DATA_THREADING import update_data
import DATA_DOWNLOADER as DOWNLOADER
import settings
import os
import datetime
import pandas as pd
import logging
from time import sleep

logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S',level=logging.DEBUG)
logging.getLogger('apscheduler').setLevel(logging.DEBUG)
SYMBOLS_FILE = settings.TRADE_SYMBOLS_ABS_PATH + "TRADE_SYMBOLS.csv"
print("SYMBOLS_FILE :",SYMBOLS_FILE)
symbols = pd.read_csv(SYMBOLS_FILE)
symbols['cash'] = symbols['cash'].map(str) + '#'
print("NUMBER OF CASH SYMBOL ",len(symbols['cash']))
print("NUMBER OF FUTURE SYMBOL ",len(symbols['future'].dropna()))
# SYMBOLS = list(symbols['cash'][0:10].values) + list(symbols['future'][0:10].values)
SYMBOLS = list(symbols['cash'].dropna().values) + list(symbols['future'].dropna().values)
print("NUMBER OF SYMBOLS SELECTED ",len(SYMBOLS))

enable_interval_15_dict = dict(time_period='15',push_pull_port=settings.PUSH_PULL_PORT,SYMBOLS=SYMBOLS)
enable_interval_30_dict = dict(time_period='30',push_pull_port=settings.PUSH_PULL_PORT,SYMBOLS=SYMBOLS)
enable_interval_60_dict = dict(time_period='60',push_pull_port=settings.PUSH_PULL_PORT,SYMBOLS=SYMBOLS)

ENABLE_INTERVAL_TIME_DICT = settings.ENABLE_INTERVAL_TIME_DICT
DISABLE_INTERVAL_TIME_DICT = settings.DISABLE_INTERVAL_TIME_DICT


def disable_interval(JOB_ID):
    sched.remove_job(JOB_ID)

def enable_interval(time_period,push_pull_port,SYMBOLS):
    JOB_ID = 'INTERVAL_JOB_' + time_period
    update_data_dict = dict(time_period=time_period,push_pull_port=push_pull_port,SYMBOLS=SYMBOLS)
    print(JOB_ID,"created")
    sched.add_job(update_data, 'interval', minutes=int(time_period),id=JOB_ID,kwargs=update_data_dict)
    update_data(**update_data_dict)

sched = BlockingScheduler()

for TIMEPERIOD in settings.TIMEPERIOD:
    upd_dict = "enable_interval_" + TIMEPERIOD + "_dict"
    upd_dict = eval(upd_dict)
    print("UPDATE_DICT ",upd_dict)
    UPDATE_CSV = settings.UPDATE_CSV_PATH + "UPDATE_CSV_" + TIMEPERIOD + '.csv'
    print("UPDATE_CSV ",UPDATE_CSV)
    UPDATE_DF = pd.read_csv(UPDATE_CSV)
    LAST_DATE = UPDATE_DF['updated_time'].value_counts().index[0]
    # LAST_DATE = "2019-10-24 14:45" #Custom checking
    print("LAST_DATE ",LAST_DATE)
    download = DOWNLOADER.Data_downloader(TIMEPERIOD,settings.PUSH_PULL_PORT[0][0],settings.PUSH_PULL_PORT[0][1])
    NEXT_DATE = download.get_next_update_date(LAST_DATE,download.START_TIME_HOUR,download.START_TIME_MINUTE,download.TIMEPERIOD_INT,download.DELAY_DICT)
    print("NEXT_DATE ",NEXT_DATE)
    TODAY_DATE = datetime.datetime.today()
    # TODAY_DATE = datetime.datetime.strptime("2019-10-25 15:17",'%Y-%m-%d %H:%M')# Custom Checking
    TODAY_DATE_STR = datetime.datetime.strftime(TODAY_DATE,'%Y-%m-%d %H:%M:%S')
    print("TODAY_DATE ",TODAY_DATE)
    start_time =  datetime.datetime.combine(datetime.datetime.strptime(str(TODAY_DATE.date()),'%Y-%m-%d')\
                                            ,datetime.time(download.START_TIME_HOUR,download.START_TIME_MINUTE)) + datetime.timedelta(minutes=30)
    end_time =  datetime.datetime.combine(datetime.datetime.strptime(str(TODAY_DATE.date()),'%Y-%m-%d')\
                                            ,datetime.time(download.END_TIME_HOUR,download.END_TIME_MINUTE)) + datetime.timedelta(minutes=60)
    print("TODAY INITIAL TIME / START TIME ",start_time)
    print("TODAY FINAL TIME / END TIME ",end_time)
    if (NEXT_DATE.date() <= TODAY_DATE.date()) :  ### This is not working when running on Sunday
        if not (start_time < TODAY_DATE < end_time) :
            print("RUN ONCE and EXIT ")
            update_data(**upd_dict)
            sleep(60)
        else :
            print("NOW")
            comp_time = datetime.datetime.combine(datetime.datetime.strptime\
                                (str(TODAY_DATE.date()),'%Y-%m-%d'),datetime.time(settings.START_TIME_HOUR,settings.START_TIME_MINUTE))
            print("comp_time ",comp_time)
            NEXT_RUN = download.get_next_update_date(str(TODAY_DATE_STR),download.START_TIME_HOUR,download.START_TIME_MINUTE,download.TIMEPERIOD_INT,download.DELAY_DICT,True)
            if NEXT_RUN is False :
                print("NEXT RUN IS FALSE")
                update_data(**upd_dict)
                sleep(20)
                if not ENABLE_INTERVAL_TIME_DICT['RUN'].get(TIMEPERIOD) is None :
                    EN_RUN_HOUR = ENABLE_INTERVAL_TIME_DICT['RUN'][TIMEPERIOD]['HOUR']
                    print("EN_RUN_HOUR ",EN_RUN_HOUR)
                    EN_RUN_MINUTE = ENABLE_INTERVAL_TIME_DICT['RUN'][TIMEPERIOD]['MINUTE']
                    print("EN_RUN_MINUTE ",EN_RUN_MINUTE)
                    sched.add_job(update_data,'cron',day_of_week='mon-fri', hour=EN_RUN_HOUR,minute=EN_RUN_MINUTE,timezone='Asia/Kolkata',kwargs=upd_dict)
            else:
                print("NEXT RUN DATE TIME ",NEXT_RUN)
                EN_HOUR = NEXT_RUN.hour
                print("EN_HOUR ",EN_HOUR)
                EN_MINUTE = NEXT_RUN.minute + ENABLE_INTERVAL_TIME_DICT[TIMEPERIOD]
                print("EN_MINUTE ",EN_MINUTE)
                DS_HOUR = DISABLE_INTERVAL_TIME_DICT[TIMEPERIOD]['HOUR']
                print("DS_HOUR ",DS_HOUR)
                DS_MINUTE = DISABLE_INTERVAL_TIME_DICT[TIMEPERIOD]['MINUTE']
                print("DS_MINUTE ",DS_MINUTE)
                DS_JOB_NAME = 'INTERVAL_JOB_' + TIMEPERIOD
                print("DS_JOB_NAME ",DS_JOB_NAME)
                if NEXT_DATE < TODAY_DATE : ## e.g. next_date is 12_oct 9:45 and now time is 12_oct 13:15
                    print("UPDAT NOW and RUN SCHEDULER and RUN ONCE AT SCHEDULER")
                    update_data(**upd_dict)
                    sched.add_job(enable_interval,'cron',day_of_week='mon-fri',hour=EN_HOUR,minute=EN_MINUTE,timezone='Asia/Kolkata',kwargs=upd_dict)
                    sched.add_job(disable_interval,'cron',day_of_week='mon-fri',hour=DS_HOUR,minute=DS_MINUTE,timezone='Asia/Kolkata',args=[DS_JOB_NAME])
                    if not ENABLE_INTERVAL_TIME_DICT['RUN'].get(TIMEPERIOD) is None :
                        EN_RUN_HOUR = ENABLE_INTERVAL_TIME_DICT['RUN'][TIMEPERIOD]['HOUR']
                        print("EN_RUN_HOUR ",EN_RUN_HOUR)
                        EN_RUN_MINUTE = ENABLE_INTERVAL_TIME_DICT['RUN'][TIMEPERIOD]['MINUTE']
                        print("EN_RUN_MINUTE ",EN_RUN_MINUTE)
                        sched.add_job(update_data,'cron',day_of_week='mon-fri', hour=EN_RUN_HOUR,minute=EN_RUN_MINUTE,timezone='Asia/Kolkata',kwargs=upd_dict)
                        sleep(20)
                elif NEXT_DATE >= TODAY_DATE : ## e.g next date is 12_oct 9:45 and and now time is 12_oct 8:20
                    print("TIME HAS NOT COME RUN SCHEDULER and RUN ONCE AT SCHEDULER ")
                    sched.add_job(enable_interval, 'cron', day_of_week='mon-fri', hour=EN_HOUR,minute=EN_MINUTE,timezone='Asia/Kolkata',kwargs=upd_dict)
                    sched.add_job(disable_interval, 'cron',day_of_week='mon-fri', hour=DS_HOUR,minute=DS_MINUTE,timezone='Asia/Kolkata',args=[DS_JOB_NAME])
                    if not ENABLE_INTERVAL_TIME_DICT['RUN'].get(TIMEPERIOD) is None :
                        EN_RUN_HOUR = ENABLE_INTERVAL_TIME_DICT['RUN'][TIMEPERIOD]['HOUR']
                        print("EN_RUN_HOUR ",EN_RUN_HOUR)
                        EN_RUN_MINUTE = ENABLE_INTERVAL_TIME_DICT['RUN'][TIMEPERIOD]['MINUTE']
                        print("EN_RUN_MINUTE ",EN_RUN_MINUTE)
                        sched.add_job(update_data,'cron',day_of_week='mon-fri', hour=EN_RUN_HOUR,minute=EN_RUN_MINUTE,timezone='Asia/Kolkata',kwargs=upd_dict)
    elif NEXT_DATE.date() > TODAY_DATE.date() :
        if len(os.sys.argv) > 1 :
            if os.sys.argv[1] == "FORCE" :
                update_data(**upd_dict)
                sleep(60)
            else:
                print("NO COMMAND ")
        else:
            print("ALREADY UPDATED ")
    else:
        print("ERROR")
    print("###########______________###############")

if len(sched.get_jobs()) > 0 :
    # print("JOBS ",sched.get_jobs())
    sched.start()
