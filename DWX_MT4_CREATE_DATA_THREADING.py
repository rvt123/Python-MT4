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
import concurrent.futures
import time

def update_data(time_period,push_pull_port,SYMBOLS):
    print("TIMEPERIOD ",time_period)
    print("PUSH_PULL_PORT ",push_pull_port)
    print("NUMBER OF THREADS ",len(push_pull_port))
    print("LEN SYMBOLS ",len(SYMBOLS))
    print()
    update_dict = dict()
    downloader_create_fn_list = []
    for push,pull in push_pull_port:
        downloader = DOWNLOADER.Data_downloader(timeperiod=time_period,push_port=push,pull_port=pull)
        downloader_create_fn_list.append(downloader.create_data_file)

    for i in range(0,2):
        SYMBOLS_FALSE = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(push_pull_port)) as executor:
            down_executor_dict = { executor.submit(downloader_create_fn_list[index % len(downloader_create_fn_list)]\
                                                            ,SYMBOLS[index]) : SYMBOLS[index] for index in range(0,len(SYMBOLS))}
            for complete_downld in concurrent.futures.as_completed(down_executor_dict):
                if i ==0 :
                    symbol_name = down_executor_dict[complete_downld]
                    updated,updated_time = complete_downld.result()
                    if updated == True:
                        update_dict[symbol_name] = [updated,updated_time]
                    else:
                        SYMBOLS_FALSE.append(symbol_name)
                else:
                    update_dict.update({down_executor_dict[complete_downld]:complete_downld.result()})

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
    print()

if __name__ == "__main__":

    print("CURRENT_DIR ",os.getcwd())
    SYMBOLS_FILE = settings.TRADE_SYMBOLS_ABS_PATH + "TRADE_SYMBOLS.csv"
    print("SYMBOLS_FILE :",SYMBOLS_FILE)
    symbols = pd.read_csv(SYMBOLS_FILE)
    symbols['cash'] = symbols['cash'].map(str) + '#'
    print("NUMBER OF CASH SYMBOL ",len(symbols['cash']))
    print("NUMBER OF FUTURE SYMBOL ",len(symbols['future'].dropna()))

    # SYMBOLS = list(symbols['cash'][0:30].values) + list(symbols['future'][0:30].values)
    SYMBOLS = list(symbols['cash'].dropna().values) + list(symbols['future'].dropna().values)
    print("Number of Symbols updating ",len(SYMBOLS))

    ## Dictionary to update latest date on which file is updated
    UPDATE_DF_RENAME_DICT = {'index':'symbol',0:'updated',1:'updated_time'}

    start = time.perf_counter()

    with concurrent.futures.ProcessPoolExecutor(max_workers=3) as executor:
        executor.submit(update_data,settings.GENERATE_DATA_FILE_TIMEPERIOD[0],settings.PUSH_PULL_PORT[:4],SYMBOLS)
        executor.submit(update_data,settings.GENERATE_DATA_FILE_TIMEPERIOD[1],settings.PUSH_PULL_PORT[4:8],SYMBOLS)
        executor.submit(update_data,settings.GENERATE_DATA_FILE_TIMEPERIOD[2],settings.PUSH_PULL_PORT[8:],SYMBOLS)

    finish = time.perf_counter()
    print(f'Finished in {round(finish-start, 2)} second(s)')
