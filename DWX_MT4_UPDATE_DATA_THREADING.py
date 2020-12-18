import pandas as pd
import numpy as np
from pandas.tseries.offsets import BDay
import datetime
import os
from math import ceil
import MT4_HISTORY_IO as DWX
import DWX_Data_Server as DWX_live
import DATA_DOWNLOADER as DOWNLOADER
import settings
import time
import concurrent.futures
from GEN_SIGNALS import Signals
DEBUG = False

def return_update_csv(csv_path,timeperiod):
    UPDATE_CSV_FILE = csv_path + "UPDATE_CSV_" +  timeperiod + '.csv'
    print("UPDATE_CSV_FILE :",UPDATE_CSV_FILE)
    if os.path.exists( UPDATE_CSV_FILE ) :
        update_df = pd.read_csv(UPDATE_CSV_FILE)
        return update_df
    else :
        return False

def download_helper(updater,update_dict,UPDATE_TIMEPERIOD,symbol,DEBUG=settings):
    last_update_time = update_dict.get(symbol,['False','False'])[1]
    print("SYMBOL UPDATE_MODE, LAST UPDATE DATE AND TIME ",symbol,last_update_time)
    if (last_update_time == 'FALSE') | (last_update_time == 'False') :
        print("DATA_FIILE DOES NT EXIST CREATING IT")
        ## CREATE HISTORY FILE HERE -> PASS DATA FILE , SYMBOL and LAST_UPDATE_TIME  AS PARAMETER
        updated , updated_time = updater.create_data_file(symbol)
        if DEBUG:
            print("updated ",updated)
            print("Updated_time ",updated_time)
        return updated,updated_time
    else:
        try:
            last_update_date = str(datetime.datetime.strptime(last_update_time,'%Y-%m-%d %H:%M')).split(' ')[0]
        except:
            last_update_date = str(datetime.datetime.strptime(last_update_time,'%Y-%m-%d %H:%M:%S')).split(' ')[0]
        DATA_FILE = settings.DATA_DIRECTORY + last_update_date + "//" + str(UPDATE_TIMEPERIOD) + "//" + symbol + '.csv'
        print("DATA_FILE :",DATA_FILE)
        if os.path.exists( DATA_FILE ) :
            updated , updated_time = updater.update_data_file(symbol,DATA_FILE,last_update_time)
            if DEBUG:
                print("updated ",updated)
                print("Updated_time ",updated_time)
            return updated,updated_time
        else :
            print("DATA FILE DOES NOT EXIST CREATING DATA FILE HERE ")
            updated , updated_time = updater.create_data_file(symbol)
            if DEBUG:
                print("updated ",updated)
                print("Updated_time ",updated_time)
            return updated,updated_time

    print("##############################")

def update_data(time_period,push_pull_port,SYMBOLS):
    print("TIMEPERIOD ",time_period)
    print("PUSH_PULL_PORT ",push_pull_port)
    print("NUMBER OF THREADS ",len(push_pull_port))
    print()
    update_df = return_update_csv(csv_path=settings.UPDATE_CSV_PATH,timeperiod=time_period)
    if update_df is False :
        print("UPDATE_FILE NT FOUND EXITING")
        exit()
    update_list = list(map(list,zip(list(update_df['updated']),list(update_df['updated_time']))))
    update_dict = dict(zip(list(update_df['symbol']),update_list))
    UPDATE_DF_RENAME_DICT = {'index':'symbol',0:'updated',1:'updated_time'}

    downloader_create_fn_list = []
    for push,pull in push_pull_port:
        print("Push_port {push} , Pull_port {pull}".format(push=push,pull=pull))
        downloader_create_fn_list.append(DOWNLOADER.Data_downloader(timeperiod=time_period,push_port=push,pull_port=pull))

    for i in range(0,2):
        SYMBOLS_FALSE = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(push_pull_port)) as executor:
            down_executor_dict = { executor.submit(download_helper,downloader_create_fn_list[index % len(downloader_create_fn_list)]\
                        ,update_dict,time_period,SYMBOLS[index]) : SYMBOLS[index] for index in range(0,len(SYMBOLS))}
            for complete_downld in concurrent.futures.as_completed(down_executor_dict):
                symbol_name = down_executor_dict[complete_downld]
                updated,updated_time = complete_downld.result()
                if updated == True:
                    update_dict[symbol_name] = [updated,updated_time]
                else:
                    SYMBOLS_FALSE.append(symbol_name)
        print("########")
        print("SYMBOLS_FALSE ",SYMBOLS_FALSE)
        print("########")
        FILES_PATH = settings.DATA_DIRECTORY + max(os.listdir(settings.DATA_DIRECTORY)) + '//' + time_period
        SYMBOLS = list(set(SYMBOLS) - set(list(map(lambda x: x.replace('.csv',''), os.listdir(FILES_PATH)))))
        SYMBOLS = list(set(SYMBOLS + SYMBOLS_FALSE))

    update_df = pd.DataFrame.from_dict(update_dict,orient='index').reset_index().rename(columns=UPDATE_DF_RENAME_DICT)
    UPDATE_DF_FILENAME = settings.UPDATE_CSV_PATH + "UPDATE_CSV_" + str(time_period) + '.csv'
    update_df.to_csv(UPDATE_DF_FILENAME,index=False)
    print("UPDATE_DF CSV CREATED ",time_period)
    if settings.SAVE_SIGNAL_DICT.get(time_period) :
        sig = Signals(time_period,telegram_notify=settings.TELEGRAM_NOTIFY_DICT.get(time_period))
        sig.save_signal("TRIPLE_CLUSTER")
        sig.save_signal("CLUSTER")
        sig.save_signal("BB_BAND")
        sig.save_signal("SINGL_CANDLESTICKS")
    print()
