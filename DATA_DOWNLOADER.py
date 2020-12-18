import pandas as pd
import numpy as np
from pandas.tseries.offsets import BDay
import datetime
import os
from math import ceil
import MT4_HISTORY_IO as DWX
import DWX_Data_Server as DWX_live
import settings

class Data_downloader():
    def __init__(self,timeperiod,push_port,pull_port):
        ## Parameter to create history and live class objects
        self.HISTORY_MODULE = "DWX"
        self.HISTORY_MODULE_CLASS = "DWX_MT4_HISTORY()"
        self.LIVE_MODULE = "DWX_live"
        self.LIVE_MODULE_CLASS = "DWX_ZeroMQ_Connector(_PUSH_PORT={push},_PULL_PORT={pull})".format(push=push_port,pull=pull_port)
        self.DEBUG = settings.DATA_DOWNLOADER_DEBUG
        # _PUSH_PORT,_PULL_PORT
        ## Paramter for saving data files
        self.DATA_DIRECTORY = settings.DATA_DIRECTORY
        self.HOLIDAY_DF = pd.read_csv(settings.HOLIDAY_CSV_PATH)
        self.HOLIDAY_DF['Date'] = self.HOLIDAY_DF['Date'].apply( lambda x : datetime.datetime.strptime(x,'%d/%m/%Y').date())

        ## Parameters for HISTORY DATA
        self.HISTORY_FILE_PATH = settings.HISTORY_FILE_PATH
        self.HISTORY_VERBOSE = settings.HISTORY_VERBOSE
        self.HISTORY_BAR = settings.HISTORY_BAR

        ## Date Generation Paramters
        self.START_TIME_HOUR = settings.START_TIME_HOUR
        self.START_TIME_MINUTE = settings.START_TIME_MINUTE
        self.END_TIME_HOUR = settings.END_TIME_HOUR
        self.END_TIME_MINUTE = settings.END_TIME_MINUTE

        ## GENERAL - TIMEPERIOD UPDATE or CREATE
        self.TIMEPERIOD_STR = timeperiod #'60','15','30',
        self.TIMEPERIOD_INT = int(self.TIMEPERIOD_STR)

        ## If history file in not present then to get date according to number of bars
        self.CANDL_M5 = 73
        self.CANDL_M15 = 25
        self.CANDL_M30 = 13
        self.CANDL_M60 = 7

        ## DELAY DICT
        self.DELAY_DICT = settings.DELAY_DICT

    def modification_date(self,symbol,frequency):
        FRQ_DEL = {30:15,60:15} ## LAST CANDLES ARE ONLY OF 15 MINS in 30 and 60
        for filepath in range(len(self.HISTORY_FILE_PATH),0,-1):
            filename = self.HISTORY_FILE_PATH[filepath-1] + symbol + self.TIMEPERIOD_STR + '.hst'
            if os.path.exists(filename):
                t = os.path.getmtime(filename)
                break

        file_date = datetime.datetime.fromtimestamp(t)
        chk_time = datetime.datetime.combine(datetime.datetime.strptime(str(file_date.date()),'%Y-%m-%d')\
                                                                    ,datetime.time(15,15))
        if file_date > chk_time :
            file_date = file_date - datetime.timedelta(minutes=FRQ_DEL.get(frequency,frequency)+70)
            # 70 because market are closing so there is some price fluctuation so after this much time_period
            # evening session wwould have been done 
        else:
            file_date = file_date - datetime.timedelta(minutes=frequency+31)
        return file_date

    def create_hist_obj(self):
        hist = eval(self.HISTORY_MODULE + "." + self.HISTORY_MODULE_CLASS)
        return hist
    def create_live_data_obj(self):
        live = eval(self.LIVE_MODULE + "." + self.LIVE_MODULE_CLASS)
        return live
    def return_valid_date(self,curr_date,date_type):
        if( (datetime.datetime.isoweekday(curr_date) == 6) | \
            (datetime.datetime.isoweekday(curr_date) == 7) | \
             (curr_date in list(self.HOLIDAY_DF['Date'].values)) ):
            if date_type == "PAST":
                return self.return_valid_date(curr_date - datetime.timedelta(days=1),date_type)
            elif date_type == "FUTURE":
                return self.return_valid_date(curr_date + datetime.timedelta(days=1),date_type)
        else:
            return curr_date

    def if_invalid_date(self,curr_date):
        if( (datetime.datetime.isoweekday(curr_date) == 6) | \
            (datetime.datetime.isoweekday(curr_date) == 7) | \
             (curr_date in list(self.HOLIDAY_DF['Date'].values)) ):
             return True
        else:
            return False

    def get_next_date_and_time(self,current_row,date_col,next_START_TIME_HOUR,next_START_TIME_MINUTE,frequency,freq_delay_dict):
        current_date = str(current_row[date_col]).split(' ')[0]
        current_time = str(current_row[date_col]).split(' ')[1]
        if (current_time == "14:45:00") | ( self.if_invalid_date( datetime.datetime.strptime(current_date,'%Y-%m-%d').date() ) ) :
            next_date = self.return_valid_date((datetime.datetime.strptime(current_date,'%Y-%m-%d') + datetime.timedelta(days=1)).date(),"FUTURE")
            next_date_time = datetime.datetime.combine(datetime.datetime.strptime(str(next_date),'%Y-%m-%d')\
                                        ,datetime.time(next_START_TIME_HOUR,next_START_TIME_MINUTE)) ## initial combined
        else:
            next_date = current_row[date_col].date()
            next_date_time = datetime.datetime.strptime(str(current_row[date_col]),'%Y-%m-%d %H:%M:%S')
            next_date_time = next_date_time + datetime.timedelta(minutes=frequency+freq_delay_dict[frequency])
        return [next_date,next_date_time]

    def get_next_update_date(self,current_date_time,next_START_TIME_HOUR,next_START_TIME_MINUTE,frequency,freq_delay_dict,MODE_CANDL=False):
        try:
            curr_dt_time = datetime.datetime.strptime(current_date_time,'%Y-%m-%d %H:%M:%S')
        except:
            curr_dt_time = datetime.datetime.strptime(current_date_time,'%Y-%m-%d %H:%M')
        current_date = str(current_date_time).split(' ')[0]
        current_time = str(current_date_time).split(' ')[1]
        if MODE_CANDL :
            all_candl = self.generate_dates("TODAY",self.START_TIME_HOUR,self.START_TIME_MINUTE,"TODAY",\
                self.END_TIME_HOUR,self.END_TIME_MINUTE,frequency,freq_delay_dict,True) + datetime.timedelta(minutes = 30)
            if len(all_candl[all_candl > curr_dt_time ]) > 0:
                next_date_time = all_candl[all_candl > curr_dt_time ][0]
                return next_date_time
            else :
                return False
        else :
            if ((current_time == "14:45:00") or (current_time == "14:45")) :
                next_date = self.return_valid_date((datetime.datetime.strptime(current_date,'%Y-%m-%d') + datetime.timedelta(days=1)).date(),"FUTURE")
                next_date_time = datetime.datetime.combine(datetime.datetime.strptime(str(next_date),'%Y-%m-%d')\
                                            ,datetime.time(next_START_TIME_HOUR,next_START_TIME_MINUTE)) ## initial combined
            else:
                try:
                    next_date = datetime.datetime.strptime(current_date_time,'%Y-%m-%d %H:%M').date()
                    next_date_time = datetime.datetime.strptime(str(current_date_time),'%Y-%m-%d %H:%M')
                except:
                    next_date = datetime.datetime.strptime(current_date_time,'%Y-%m-%d %H:%M:%S').date()
                    next_date_time = datetime.datetime.strptime(str(current_date_time),'%Y-%m-%d %H:%M:%S')
            next_date_time = next_date_time + datetime.timedelta(minutes=30+frequency+freq_delay_dict[frequency])
            return next_date_time

    def create_workpath(self,workpath):
        if not os.path.exists(workpath):
            try:
                os.makedirs(workpath)
                print ('created', workpath)
            except FileExistsError:
                print('Directory not created,may be created by other thread')
        else:
            pass
    def date_at_bars(self,numr_of_bar,end_date,frequency):
        DAYS = 0
        if end_date == "TODAY":
            if datetime.datetime.today() > datetime.datetime.combine(datetime.datetime.strptime(str(datetime.datetime.today().date()),'%Y-%m-%d')\
                                        ,datetime.time(self.END_TIME_HOUR,self.END_TIME_MINUTE)):
                DAYS = DAYS -1
            end_date = datetime.datetime.today().date()
        if frequency == 5:
            frequency = self.CANDL_M5
        elif frequency == 15:
            frequency = self.CANDL_M15
        elif frequency == 30:
            frequency == self.CANDL_M30
        elif frequency == 60:
            frequency = self.CANDL_M60
        else:
            return False
        DAYS = DAYS + ceil(numr_of_bar/frequency)
        if ( (datetime.datetime.isoweekday(end_date) == 6) | (datetime.datetime.isoweekday(end_date) == 7) ):
            DAYS = DAYS + 1
        return (datetime.datetime.today().date() - BDay(DAYS)).date()

    def generate_dates(self,start_date,start_time_hour,start_time_min,end_date,end_date_hour,end_date_min,frequency,delay_dict,FULL=False):
        FRQ_DEL = {30:15,60:15} ## LAST CANDLES ARE ONLY OF 15 MINS in 30 and 60
        if start_date == "TODAY":
            START_DATE = str(datetime.datetime.today().date())
        else:
            START_DATE = start_date
        if end_date == "TODAY":
            END_DATE = str(datetime.datetime.today().date())
        else:
            END_DATE = end_date
        if self.DEBUG:
            print("Start_date : ",START_DATE)
            print("End_date : ",END_DATE)
        days_count = int((datetime.datetime.strptime(END_DATE,'%Y-%m-%d') - datetime.datetime.strptime(START_DATE,'%Y-%m-%d')).total_seconds()/(60*60*24))
        date_list = [datetime.datetime.strptime(START_DATE,'%Y-%m-%d') + datetime.timedelta(days=x) for x in \
                                       range(0,days_count+1)]
        date_days = [datetime.datetime.isoweekday(x) for x in  date_list]
        dates = np.array(date_list)[~((np.array(date_days)== 6)|(np.array(date_days)== 7)) ] # Excluding Saturday and Sunday
        # if self.DEBUG:
            # print("Dates : ",dates)
        comb_date = []
        for date in dates:
            START_DATE = str(date.date())
            START_TIME_HOUR = start_time_hour
            START_TIME_MINUTE = start_time_min

            END_DATE = str(date.date())
            END_TIME_HOUR = end_date_hour
            END_TIME_MINUTE = end_date_min
            FREQUENCY_DOWNLOAD = frequency

            start_time =  datetime.datetime.combine(datetime.datetime.strptime(START_DATE,'%Y-%m-%d')\
                                                                ,datetime.time(START_TIME_HOUR,START_TIME_MINUTE))
            end_time =  datetime.datetime.combine(datetime.datetime.strptime(END_DATE,'%Y-%m-%d')\
                                                                ,datetime.time(END_TIME_HOUR,END_TIME_MINUTE))
            date_list = [start_time + datetime.timedelta(minutes=FREQUENCY_DOWNLOAD*x) for x in \
                                           range(0, int(((end_time - start_time).total_seconds()/60)/FREQUENCY_DOWNLOAD + 1) )]

            comb_date += date_list
        comb_date = np.array(comb_date)
        if FULL :
            return comb_date
        else:
            if end_date == "TODAY":
                today_date_str = datetime.datetime.strftime(datetime.datetime.today(),'%Y-%m-%d %H:%M')
                today_date = datetime.datetime.strptime(today_date_str,'%Y-%m-%d %H:%M')
                chk_time = datetime.datetime.combine(datetime.datetime.strptime(str(datetime.datetime.today().date()),'%Y-%m-%d')\
                                                                       ,datetime.time(15,30))
                if today_date > chk_time :
                    completed_candl_time = today_date - datetime.timedelta(minutes=FRQ_DEL.get(frequency,frequency)+delay_dict[frequency]+30)
                else:
                    completed_candl_time = today_date - datetime.timedelta(minutes=frequency+delay_dict[frequency]+30)
                ## 30 is for time difference btetween mt4 and local time
                comb_date = comb_date[comb_date <= completed_candl_time]
            return comb_date

    def create_data_file(self,symbol):
        history_df = pd.DataFrame()
        for filepath in range(0,len(self.HISTORY_FILE_PATH)):
            FILENAME = self.HISTORY_FILE_PATH[filepath] + symbol + self.TIMEPERIOD_STR + '.hst'
            if self.DEBUG:
                print("HISTORY_FILENAME :",FILENAME)
            ## creating hist object
            hist_data = self.create_hist_obj()
            if history_df.empty:
                history_df = hist_data.get_history(FILENAME,symbol,self.TIMEPERIOD_STR,self.HISTORY_BAR,self.HISTORY_VERBOSE)
            else:
                history_df = pd.concat([history_df,hist_data.get_history(FILENAME,symbol,self.TIMEPERIOD_STR,self.HISTORY_BAR,self.HISTORY_VERBOSE)],axis=0).reset_index(drop=True)
        if history_df.empty:
            if self.DEBUG:
                print("HISTORY FILE DOES NOT EXIST")
            CHECK_TIME = datetime.datetime.combine(datetime.datetime.now(),datetime.time(self.START_TIME_HOUR,self.START_TIME_MINUTE)) +\
                        datetime.timedelta(minutes=self.TIMEPERIOD_INT+self.DELAY_DICT.get(self.TIMEPERIOD_INT)+30)
            if self.DEBUG :
                print("CHECK_TIME",CHECK_TIME)
            if datetime.datetime.now() < CHECK_TIME :
                END_DATE = str(self.return_valid_date(datetime.datetime.today().date() - datetime.timedelta(days=1),'PAST'))
            else:
                END_DATE = "TODAY"
            START_DATE_in_dt = self.date_at_bars(self.HISTORY_BAR,"TODAY",self.TIMEPERIOD_INT)
            START_DATE = str(START_DATE_in_dt)
            if self.DEBUG:
                print(" START_DATE_in_dt ",START_DATE)
            date_gen = self.generate_dates(START_DATE,self.START_TIME_HOUR,self.START_TIME_MINUTE,END_DATE,self.END_TIME_HOUR,self.END_TIME_MINUTE,self.TIMEPERIOD_INT,self.DELAY_DICT,False)
            # print(date_gen)
            live_data = self.create_live_data_obj()
            START_STRING = datetime.datetime.strftime(START_DATE_in_dt,'%Y.%m.%d 08:45:00')
            END_STRING = datetime.datetime.strftime(date_gen[-1],'%Y.%m.%d %H:%M:00')
            data_df = live_data._DWX_MTX_SEND_MARKETDATA_REQUEST_(_symbol=symbol,_timeframe=self.TIMEPERIOD_STR,_start=START_STRING,_end=END_STRING)
            if data_df is False:
                if self.DEBUG:
                    print("COULD NOT SAVE FILE, PLS TRY AGAIN")
                return False,False
            else:
                data_df['date_time'] = data_df['date_time'].apply(lambda x:datetime.datetime.strptime(x,'%Y.%m.%d %H:%M'))
                data_df = data_df.drop_duplicates('date_time',keep='last')
                data_df = data_df[data_df['symbol'] == symbol]
                if ( str(data_df['date_time'].iloc[-1]) >= str(date_gen[-1]) ) :
                    LAST_VALID_DATE = str(self.return_valid_date(data_df['date_time'].iloc[-1].date(),'PAST'))
                    WORKPATH = self.DATA_DIRECTORY + LAST_VALID_DATE + "//" + self.TIMEPERIOD_STR + "//"
                    update_date_time = datetime.datetime.strftime(data_df.iloc[-1]['date_time'],'%Y-%m-%d %H:%M')
                    self.create_workpath(WORKPATH)
                    FINAL_DF_FILENAME = WORKPATH + symbol + '.csv'
                    data_df.to_csv(FINAL_DF_FILENAME,index=False)
                    if self.DEBUG:
                        print("DATA CSV is saved at ",FINAL_DF_FILENAME)
                    return True,update_date_time
                else:
                    return False,False
        else:
            history_df = history_df.drop_duplicates(subset='date_time')
            history_df['date_time'] = history_df['date_time'].apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))
            history_df.insert(0,'symbol',symbol)
            mod_date = self.modification_date(symbol,self.TIMEPERIOD_INT)
            history_df = history_df[history_df['date_time'] < mod_date ]
            history_df = history_df[history_df['symbol'] == symbol]
            next_date,next_date_time = self.get_next_date_and_time(history_df.iloc[-1],'date_time',self.START_TIME_HOUR,self.START_TIME_MINUTE,self.TIMEPERIOD_INT,self.DELAY_DICT)
            if self.DEBUG:
                print("LAST DATE IN HISTORY FILE  :",history_df.iloc[-1]['date_time'])
                print("NEXT_DATE_TIME :",str(next_date_time))
            if next_date_time > datetime.datetime.today():
                if self.DEBUG:
                    print("HISTORY FILE IS UPDATED SAVING THE FILE")
                LAST_VALID_DATE = str(self.return_valid_date(history_df.iloc[-1]['date_time'].date(),'PAST'))
                WORKPATH = self.DATA_DIRECTORY + LAST_VALID_DATE + "//" + self.TIMEPERIOD_STR + "//"
                update_date_time = datetime.datetime.strftime(history_df.iloc[-1]['date_time'],'%Y-%m-%d %H:%M')
                self.create_workpath(WORKPATH)
                FINAL_DF_FILENAME = WORKPATH + symbol + '.csv'
                history_df.to_csv(FINAL_DF_FILENAME,index=False)
                if self.DEBUG:
                    print("DATA CSV is saved at ",FINAL_DF_FILENAME)
                return True,update_date_time
            else:
                START_DATE = str(next_date)
                CHECK_TIME = datetime.datetime.combine(datetime.datetime.now(),datetime.time(self.START_TIME_HOUR,self.START_TIME_MINUTE)) +\
                            datetime.timedelta(minutes=self.TIMEPERIOD_INT+self.DELAY_DICT.get(self.TIMEPERIOD_INT)+30)
                if self.DEBUG :
                    print("CHECK_TIME",CHECK_TIME)
                if datetime.datetime.now() < CHECK_TIME :
                    END_DATE = str(self.return_valid_date(datetime.datetime.today().date() - datetime.timedelta(days=1),'PAST'))
                else:
                    END_DATE = "TODAY"
                date_gen = self.generate_dates(START_DATE,self.START_TIME_HOUR,self.START_TIME_MINUTE,END_DATE,self.END_TIME_HOUR,self.END_TIME_MINUTE,self.TIMEPERIOD_INT,self.DELAY_DICT,False)
                if self.DEBUG :
                    print(date_gen)
                live_data = self.create_live_data_obj()
                START_STRING = datetime.datetime.strftime(next_date,'%Y.%m.%d 08:45:00')
                END_STRING = datetime.datetime.strftime(date_gen[-1],'%Y.%m.%d %H:%M:00')
                data_df = live_data._DWX_MTX_SEND_MARKETDATA_REQUEST_(_symbol=symbol,_timeframe=self.TIMEPERIOD_STR,_start=START_STRING,_end=END_STRING)
                if data_df is False :
                    if self.DEBUG:
                        print("COULD NOT SAVE FILE, PLS TRY AGAIN")
                    return False,False
                else:
                    data_df['date_time'] = data_df['date_time'].apply(lambda x:datetime.datetime.strptime(x,'%Y.%m.%d %H:%M'))
                    final_df = pd.concat([history_df,data_df]).reset_index(drop=True)
                    final_df = final_df.drop_duplicates('date_time',keep='last')
                    final_df = final_df[final_df['symbol'] == symbol]
                    if ( str(final_df['date_time'].iloc[-1]) >= str(date_gen[-1]) ) :
                        update_date_time = datetime.datetime.strftime(date_gen[-1],'%Y-%m-%d %H:%M')
                        LAST_VALID_DATE = str(self.return_valid_date(final_df['date_time'].iloc[-1].date(),'PAST'))
                        WORKPATH = self.DATA_DIRECTORY + LAST_VALID_DATE + "//" + self.TIMEPERIOD_STR + "//"
                        self.create_workpath(WORKPATH)
                        FINAL_DF_FILENAME = WORKPATH + symbol + '.csv'
                        final_df.to_csv(FINAL_DF_FILENAME,index=False)
                        print(FINAL_DF_FILENAME)
                        if self.DEBUG:
                            print("DATA CSV is saved at ",FINAL_DF_FILENAME)
                        return True,update_date_time
                    else:
                        return False,False

    def update_data_file(self,symbol,data_file,last_update_time):
        try:
            last_update_time =  datetime.datetime.strptime(last_update_time,'%Y-%m-%d %H:%M')
        except:
            last_update_time =  datetime.datetime.strptime(last_update_time,'%Y-%m-%d %H:%M:%S')
        DATA_FILE = pd.read_csv(data_file)
        DATA_FILE['date_time'] = DATA_FILE['date_time'].apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))
        last_date = last_update_time.date()
        if self.DEBUG :
            print(" LAST UPDATE DATE AND TIME ",last_update_time)
            print("LAST UPDATE DATE ",last_date)
        if last_update_time >= datetime.datetime.combine(datetime.datetime.strptime(str(last_date),'%Y-%m-%d')\
                                    ,datetime.time(self.END_TIME_HOUR,self.END_TIME_MINUTE)):
            START_DATE_in_dt = self.return_valid_date((datetime.datetime.strptime(str(last_date),'%Y-%m-%d') + datetime.timedelta(days=1)).date(),"FUTURE")
            if self.DEBUG :
                print("START_DATE ",START_DATE_in_dt)
        else:
            START_DATE_in_dt = last_date
            if self.DEBUG :
                print("START_DATE ",START_DATE_in_dt)
        CHECK_TIME = datetime.datetime.combine(datetime.datetime.now(),datetime.time(self.START_TIME_HOUR,self.START_TIME_MINUTE)) +\
                    datetime.timedelta(minutes=self.TIMEPERIOD_INT+self.DELAY_DICT.get(self.TIMEPERIOD_INT)+30)
        if self.DEBUG :
            print("CHECK_TIME",CHECK_TIME)
        if datetime.datetime.now() < CHECK_TIME :
            END_DATE = str(self.return_valid_date(datetime.datetime.today().date() - datetime.timedelta(days=1),'PAST'))
        else:
            END_DATE = "TODAY"
        START_DATE = str(START_DATE_in_dt)
        date_gen = self.generate_dates(START_DATE,self.START_TIME_HOUR,self.START_TIME_MINUTE,END_DATE,self.END_TIME_HOUR,self.END_TIME_MINUTE,self.TIMEPERIOD_INT,self.DELAY_DICT,False)
        num_bars = len(date_gen)
        if num_bars == 0 :
            print("FILE IS ALREADY UPDATED ")
            return True,last_update_time
        else:
            history_df = pd.DataFrame()
            for filepath in range(0,len(self.HISTORY_FILE_PATH)):
                FILENAME = self.HISTORY_FILE_PATH[filepath] + symbol + self.TIMEPERIOD_STR + '.hst'
                if self.DEBUG:
                    print("HISTORY_FILENAME :",FILENAME)
                ## creating hist object
                hist_data = self.create_hist_obj()
                if history_df.empty:
                    history_df = hist_data.get_history(FILENAME,symbol,self.TIMEPERIOD_STR,self.HISTORY_BAR,self.HISTORY_VERBOSE)
                else:
                    history_df = pd.concat([history_df,hist_data.get_history(FILENAME,symbol,self.TIMEPERIOD_STR,self.HISTORY_BAR,self.HISTORY_VERBOSE)],axis=0).reset_index(drop=True)
            if history_df.empty:
                if self.DEBUG:
                    print("HISTORY FILE DOES NOT EXIST")
                CHECK_TIME = datetime.datetime.combine(datetime.datetime.now(),datetime.time(self.START_TIME_HOUR,self.START_TIME_MINUTE)) +\
                            datetime.timedelta(minutes=self.TIMEPERIOD_INT+self.DELAY_DICT.get(self.TIMEPERIOD_INT)+30)
                if self.DEBUG :
                    print("CHECK_TIME",CHECK_TIME)
                if datetime.datetime.now() < CHECK_TIME :
                    END_DATE = str(self.return_valid_date(datetime.datetime.today().date() - datetime.timedelta(days=1),'PAST'))
                else:
                    END_DATE = "TODAY"
                START_DATE_in_dt = last_date
                START_DATE = str(START_DATE_in_dt)
                if self.DEBUG:
                    print(" START_DATE_in_dt ",START_DATE)
                date_gen = self.generate_dates(START_DATE,self.START_TIME_HOUR,self.START_TIME_MINUTE,END_DATE,self.END_TIME_HOUR,self.END_TIME_MINUTE,self.TIMEPERIOD_INT,self.DELAY_DICT,False)
                if self.DEBUG :
                    print(date_gen)
                live_data = self.create_live_data_obj()
                START_STRING = datetime.datetime.strftime(START_DATE_in_dt,'%Y.%m.%d 08:45:00')
                END_STRING = datetime.datetime.strftime(date_gen[-1],'%Y.%m.%d %H:%M:00')
                data_df = live_data._DWX_MTX_SEND_MARKETDATA_REQUEST_(_symbol=symbol,_timeframe=self.TIMEPERIOD_STR,_start=START_STRING,_end=END_STRING)
                if data_df is False:
                    if self.DEBUG:
                        print("COULD NOT SAVE FILE, PLS TRY AGAIN")
                    return False,False
                else:
                    data_df['date_time'] = data_df['date_time'].apply(lambda x:datetime.datetime.strptime(x,'%Y.%m.%d %H:%M'))
                    data_df = pd.concat([DATA_FILE,data_df]).reset_index(drop=True)
                    data_df = data_df.drop_duplicates('date_time',keep='last')
                    data_df = data_df[data_df['symbol'] == symbol]
                    if ( str(data_df['date_time'].iloc[-1]) >= str(date_gen[-1]) ) :
                        LAST_VALID_DATE = str(self.return_valid_date(data_df['date_time'].iloc[-1].date(),'PAST'))
                        WORKPATH = self.DATA_DIRECTORY + LAST_VALID_DATE + "//" + self.TIMEPERIOD_STR + "//"
                        update_date_time = datetime.datetime.strftime(data_df.iloc[-1]['date_time'],'%Y-%m-%d %H:%M')
                        self.create_workpath(WORKPATH)
                        FINAL_DF_FILENAME = WORKPATH + symbol + '.csv'
                        data_df.to_csv(FINAL_DF_FILENAME,index=False)
                        if self.DEBUG:
                            print("DATA CSV is saved at [MODE 1]",FINAL_DF_FILENAME)
                        return True,update_date_time
                    else:
                        return False,False
            else:
                history_df = history_df.drop_duplicates(subset='date_time')
                history_df['date_time'] = history_df['date_time'].apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))
                history_df.insert(0,'symbol',symbol)
                mod_date = self.modification_date(symbol,self.TIMEPERIOD_INT)
                # print("MODIFICATION DATE ",mod_date)
                history_df = history_df[history_df['date_time'] < mod_date ]
                if self.DEBUG:
                    print("LAST DATE IN HISTORY FILE  :",history_df.iloc[-1]['date_time'])
                next_date,next_date_time = self.get_next_date_and_time(history_df.iloc[-1],'date_time',self.START_TIME_HOUR,self.START_TIME_MINUTE,self.TIMEPERIOD_INT,self.DELAY_DICT)
                if self.DEBUG:
                    print("NEXT_DATE_TIME :",str(next_date_time))
                if next_date_time > datetime.datetime.today():
                    if self.DEBUG:
                        print("HISTORY FILE IS UPDATED SAVING THE FILE")
                    history_df = pd.concat([DATA_FILE,history_df]).reset_index(drop=True)
                    history_df = history_df.drop_duplicates('date_time',keep='last')
                    history_df = history_df[history_df['symbol'] == symbol]
                    if ( str(history_df['date_time'].iloc[-1]) >= str(date_gen[-1]) ) :
                        LAST_VALID_DATE = str(self.return_valid_date(history_df['date_time'].iloc[-1].date(),'PAST'))
                        WORKPATH = self.DATA_DIRECTORY + LAST_VALID_DATE + "//" + self.TIMEPERIOD_STR + "//"
                        update_date_time = datetime.datetime.strftime(history_df.iloc[-1]['date_time'],'%Y-%m-%d %H:%M')
                        self.create_workpath(WORKPATH)
                        FINAL_DF_FILENAME = WORKPATH + symbol + '.csv'
                        history_df.to_csv(FINAL_DF_FILENAME,index=False)
                        if self.DEBUG:
                            print("DATA CSV is saved at [MODE 2]",FINAL_DF_FILENAME)
                        return True,update_date_time
                    else:
                        return False,False
                else:
                    START_DATE = str(next_date)
                    CHECK_TIME = datetime.datetime.combine(datetime.datetime.now(),datetime.time(self.START_TIME_HOUR,self.START_TIME_MINUTE)) +\
                                datetime.timedelta(minutes=self.TIMEPERIOD_INT+self.DELAY_DICT.get(self.TIMEPERIOD_INT)+30)
                    if self.DEBUG :
                        print("CHECK_TIME",CHECK_TIME)
                    if datetime.datetime.now() < CHECK_TIME :
                        END_DATE = str(self.return_valid_date(datetime.datetime.today().date() - datetime.timedelta(days=1),'PAST'))
                    else:
                        END_DATE = "TODAY"
                    date_gen = self.generate_dates(START_DATE,self.START_TIME_HOUR,self.START_TIME_MINUTE,END_DATE,self.END_TIME_HOUR,self.END_TIME_MINUTE,self.TIMEPERIOD_INT,self.DELAY_DICT,False)
                    # if self.DEBUG :
                        # print(date_gen)
                    live_data = self.create_live_data_obj()
                    START_STRING = datetime.datetime.strftime(next_date,'%Y.%m.%d 08:45:00')
                    END_STRING = datetime.datetime.strftime(date_gen[-1],'%Y.%m.%d %H:%M:00')
                    data_df = live_data._DWX_MTX_SEND_MARKETDATA_REQUEST_(_symbol=symbol,_timeframe=self.TIMEPERIOD_STR,_start=START_STRING,_end=END_STRING)
                    if data_df is False :
                        if self.DEBUG:
                            print("COULD NOT SAVE FILE, PLS TRY AGAIN")
                        return False,False
                    else:
                        data_df['date_time'] = data_df['date_time'].apply(lambda x:datetime.datetime.strptime(x,'%Y.%m.%d %H:%M'))
                        final_df = pd.concat([DATA_FILE,history_df,data_df]).reset_index(drop=True)
                        final_df = final_df.drop_duplicates('date_time',keep='last')
                        final_df = final_df[final_df['symbol'] == symbol]
                        if ( str(final_df['date_time'].iloc[-1]) >= str(date_gen[-1]) ) :
                            LAST_VALID_DATE = str(self.return_valid_date(final_df['date_time'].iloc[-1].date(),'PAST'))
                            WORKPATH = self.DATA_DIRECTORY + LAST_VALID_DATE + "//" + self.TIMEPERIOD_STR + "//"
                            update_date_time = datetime.datetime.strftime(final_df.iloc[-1]['date_time'],'%Y-%m-%d %H:%M')
                            self.create_workpath(WORKPATH)
                            FINAL_DF_FILENAME = WORKPATH + symbol + '.csv'
                            final_df.to_csv(FINAL_DF_FILENAME,index=False)
                            if self.DEBUG:
                                print("DATA CSV is saved at [MODE 3]",FINAL_DF_FILENAME)
                            return True,update_date_time
                        else:
                            return False,False
    ### READ UPDATE DF ONLY ONCE , PASS IY AS PARAMETERS TO THE UPDATE_DATA_FILE FILE
