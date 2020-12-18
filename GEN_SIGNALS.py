import pandas as pd
import numpy as np
import datetime
import os
import settings
import talib
import requests
from time import sleep

class Signals():
    def __init__(self,timeperiod,telegram_notify=True):
        self.TIMEPERIOD = timeperiod
        self.FILES_PATH = settings.DATA_DIRECTORY + max(os.listdir(settings.DATA_DIRECTORY)) + '//' + self.TIMEPERIOD
        print(self.FILES_PATH)
        self.FILES_LIST = os.listdir(self.FILES_PATH)
        self.CLUSTER_PATH = "C://Users//raghu//Desktop//Create_data//SIGNALS//" + self.TIMEPERIOD +"//CLUSTER//"
        self.TRIPLE_CLUSTER_PATH = "C://Users//raghu//Desktop//Create_data//SIGNALS//" + self.TIMEPERIOD +"//TRIPLE_CLUSTER//"
        self.BB_BAND_PATH = "C://Users//raghu//Desktop//Create_data//SIGNALS//" + self.TIMEPERIOD +"//BB_BAND//"
        self.SINGL_CANDLSTICKS_PATH = "C://Users//raghu//Desktop//Create_data//SIGNALS//" + self.TIMEPERIOD +"//SINGL_CANDLSTICKS//"
        self.T_API = '952694214:AAGnE_caWQ1xBrNqkAsODRM-FgMo4YuqRu8'
        self.T_CHAT_ID = "527900885"
        self.T_RETRY_COUNT = 3
        self.T_RETRY_SLEEP = 20
        self.T_NOTIFY = telegram_notify
        self.URL = "https://api.telegram.org/bot" + self.T_API + \
                "/sendMessage?chat_id=" + self.T_CHAT_ID + "&text="
        self.SINGL_CANDLSTICK_COUNT = 22
        self.MAX_CANDL_CANDLSTICKS = 150
        self.CANDL_PATTRN = ['SHOOTING_STAR','HANGINGMAN','INV_HAMMER','HAMMER','DOZI']
        self.TOLERANCE = 0
        self.ALL_DF_COLUMNS = ['symbol','date_time','open','high','low','close','volume','EMA_20','EMA_50',\
                          'EMA_100','EMA_200','EMA_20_EMA_50','EMA_20_EMA_100','EMA_20_EMA_200',\
                          'EMA_20_close','EMA_50_EMA_100','EMA_50_EMA_200', 'EMA_50_close',\
                          'EMA_100_EMA_200', 'EMA_100_close','EMA_200_close', 'CLUSTER',\
                          'TRIPLE_CLUSTER', 'U_BAND','M_BAND','L_BAND','Bands_Width',\
                          'Bands_signal_SMA14','Bands_signal_SMA10','Bands_signal','SINGL_CANDLSTICKS',\
                          'CANDL_RATIO_'+str(self.SINGL_CANDLSTICK_COUNT),'High_Low_300','High_Low_150']
        self.ALL_DF = pd.DataFrame(columns=self.ALL_DF_COLUMNS)
        self.SINGL_CANDLSTCKS_DEBUG = False

    def singl_candlstcks_pttrn(self,row):
        if (row['open'] > row['close']): ## RED CANDL
            UPPR_CANDLSTCKS_SHADOW = (row['high'] - row['open'])/( row['high'] - row['low'] )*100
            CANDLSTCKS_BODY = (row['open'] - row['close'])/( row['high'] - row['low'] )*100
            LOWR_CANDLSTCKS_SHADOW = (row['close'] - row['low'])/( row['high'] - row['low'] )*100
            if self.SINGL_CANDLSTCKS_DEBUG:
                print("ind,UPPR,BODY,LOW",row['index'],round(UPPR_CANDLSTCKS_SHADOW,2)\
                  ,round(CANDLSTCKS_BODY,2),round(LOWR_CANDLSTCKS_SHADOW,2))
            if UPPR_CANDLSTCKS_SHADOW == 0 and LOWR_CANDLSTCKS_SHADOW == 0 :
                return 'NONE1'
            elif UPPR_CANDLSTCKS_SHADOW == 0 :
                if ( (CANDLSTCKS_BODY/LOWR_CANDLSTCKS_SHADOW) > 0.34 \
                    and (CANDLSTCKS_BODY/LOWR_CANDLSTCKS_SHADOW) < 0.53 ):
                    return 'SHOOTING_STAR'
                else:
                    return 'NONE2'#print("Ratio",(round(CANDLSTCKS_BODY/LOWR_CANDLSTCKS_SHADOW,2)))
            elif LOWR_CANDLSTCKS_SHADOW == 0:
                if ( (CANDLSTCKS_BODY / UPPR_CANDLSTCKS_SHADOW) > 0.34 \
                    and ( CANDLSTCKS_BODY / UPPR_CANDLSTCKS_SHADOW) < 0.53 ):
                    return 'HANGINGMAN'
                else:
                    return 'NONE3'
            else:
                if ( ((UPPR_CANDLSTCKS_SHADOW + CANDLSTCKS_BODY)/LOWR_CANDLSTCKS_SHADOW) > 0.34 \
                    and ((UPPR_CANDLSTCKS_SHADOW + CANDLSTCKS_BODY)/LOWR_CANDLSTCKS_SHADOW) < 0.53 \
                    and UPPR_CANDLSTCKS_SHADOW < 10):
                    return 'SHOOTING_STAR'
                elif ( ((LOWR_CANDLSTCKS_SHADOW + CANDLSTCKS_BODY)/UPPR_CANDLSTCKS_SHADOW) > 0.34 \
                      and ((LOWR_CANDLSTCKS_SHADOW + CANDLSTCKS_BODY)/UPPR_CANDLSTCKS_SHADOW) < 0.53 \
                      and LOWR_CANDLSTCKS_SHADOW < 10):
                    return 'HANGINGMAN'
                else:
                    return 'NONE4'#print("R1",round(((LOWR_CANDLSTCKS_SHADOW + CANDLSTCKS_BODY)/UPPR_CANDLSTCKS_SHADOW),2)),\
                                    #print("R2",round(((UPPR_CANDLSTCKS_SHADOW + CANDLSTCKS_BODY)/LOWR_CANDLSTCKS_SHADOW),2))
        elif ( row['open'] < row['close'] ):  ## GREEN CANDL
            UPPR_CANDLSTCKS_SHADOW = (row['high'] - row['close'])/( row['high'] - row['low'] )*100
            CANDLSTCKS_BODY = (row['close'] - row['open'])/( row['high'] - row['low'] )*100
            LOWR_CANDLSTCKS_SHADOW = (row['open'] - row['low'])/( row['high'] - row['low'] )*100
            if self.SINGL_CANDLSTCKS_DEBUG:
                print("ind,UPPR,BODY,LOW",row['index'],round(UPPR_CANDLSTCKS_SHADOW,2)\
                  ,round(CANDLSTCKS_BODY,2),round(LOWR_CANDLSTCKS_SHADOW,2))
            if UPPR_CANDLSTCKS_SHADOW == 0 and LOWR_CANDLSTCKS_SHADOW == 0 :
                return 'NONE5'
            elif UPPR_CANDLSTCKS_SHADOW == 0 :
                if ( (CANDLSTCKS_BODY/LOWR_CANDLSTCKS_SHADOW) > 0.34 \
                    and ( CANDLSTCKS_BODY / LOWR_CANDLSTCKS_SHADOW) < 0.53 ):
                    return 'INV_HAMMER'
                else:
                    return 'NONE6'
            elif LOWR_CANDLSTCKS_SHADOW == 0:
                if ( (CANDLSTCKS_BODY / UPPR_CANDLSTCKS_SHADOW) > 0.34 \
                    and ( CANDLSTCKS_BODY / UPPR_CANDLSTCKS_SHADOW) < 0.53 ):
                    return 'HAMMER'
                else:
                    return 'NONE7'
            else:
                if ( ((UPPR_CANDLSTCKS_SHADOW + CANDLSTCKS_BODY)/LOWR_CANDLSTCKS_SHADOW) > 0.34 \
                    and ((UPPR_CANDLSTCKS_SHADOW + CANDLSTCKS_BODY)/LOWR_CANDLSTCKS_SHADOW) < 0.53 \
                    and UPPR_CANDLSTCKS_SHADOW < 10):
                    return 'INV_HAMMER'
                elif ( ((LOWR_CANDLSTCKS_SHADOW + CANDLSTCKS_BODY)/UPPR_CANDLSTCKS_SHADOW) > 0.34 \
                      and ((LOWR_CANDLSTCKS_SHADOW + CANDLSTCKS_BODY)/UPPR_CANDLSTCKS_SHADOW) < 0.53 \
                      and LOWR_CANDLSTCKS_SHADOW < 10):
                    return 'HAMMER'
                else:
                    return 'NONE9'
        elif ( row['open'] == row['close'] ):
            return 'DOZI'
        else:
            return 'NONE10'

    def return_candlstick_data(self,df,CANDL_PATTRN,numbr_candl,max_candl,tolerance):
        index_low = (df.iloc[-1]['low']*(1-tolerance) <= np.array(list(df.iloc[-max_candl:]['low'].values))[::-1]).argmin()
        index_high = (df.iloc[-1]['high']*(1+tolerance) >= np.array(list(df.iloc[-max_candl:]['high'].values))[::-1]).argmin()
        PATTERN = self.singl_candlstcks_pttrn(df.iloc[-1])
        if PATTERN in CANDL_PATTRN :
            if (index_low == 0) and (PATTERN in ['INV_HAMMER','HAMMER','DOZI']):
                new_df = df.iloc[-numbr_candl:].copy()
                new_df.loc[ new_df['open'] > new_df['close'] ,'COLOUR'] = 'RED'
                new_df.loc[ new_df['open'] < new_df['close'] ,'COLOUR'] = 'GREEN'
                green = (new_df['COLOUR'] == "GREEN").sum()
                red = (new_df['COLOUR'] == "RED").sum()
                ratio = round(red/green,2)
                singl_candlstick_dict = {'SINGL_CANDLSTICKS':PATTERN,'CANDL_RATIO_'+str(numbr_candl):ratio}
                return singl_candlstick_dict
            elif (index_high == 0) and (PATTERN in ['SHOOTING_STAR','HANGINGMAN','DOZI']):
                new_df = df.iloc[-numbr_candl:].copy()
                new_df.loc[ new_df['open'] > new_df['close'] ,'COLOUR'] = 'RED'
                new_df.loc[ new_df['open'] < new_df['close'] ,'COLOUR'] = 'GREEN'
                green = (new_df['COLOUR'] == "GREEN").sum()
                red = (new_df['COLOUR'] == "RED").sum()
                ratio = round(green/red,2)
                singl_candlstick_dict = {'SINGL_CANDLSTICKS':PATTERN,'CANDL_RATIO_'+str(numbr_candl):ratio}
                return singl_candlstick_dict
            else :
                singl_candlstick_dict = {'SINGL_CANDLSTICKS':PATTERN,'CANDL_RATIO_'+str(numbr_candl):False}
                return singl_candlstick_dict

        else:
            singl_candlstick_dict = {'SINGL_CANDLSTICKS':PATTERN,'CANDL_RATIO_'+str(numbr_candl):False}
            return singl_candlstick_dict

    def TeleGram_Notify(self,message):
        now = datetime.datetime.now()
        if (now.hour >= 12):
            ampm_tag = 'pm'
            hour = now.hour - 12
        else:
            ampm_tag = 'am'
            hour = now.hour
        MSG = str(hour) + ':' + str(now.minute) + ampm_tag + ' ' + self.TIMEPERIOD +'\n' + message
#         print(MSG)
        MSG = MSG.replace(' ','%20')
        MSG = MSG.replace('#','%23')
        try:
            response = requests.get(self.URL + MSG, timeout=10)
            print("Message Sent")
            return True
        except:
            print("Message could not be sent")
            return False

    def gen_signals_df(self):
        EMA_list = ['EMA_20','EMA_50','EMA_100','EMA_200','close']
        for file in self.FILES_LIST:
            df = pd.read_csv(self.FILES_PATH + '//' + file)
            df['EMA_20'] = talib.EMA(df['close'],timeperiod=20)
            df['EMA_50'] = talib.EMA(df['close'],timeperiod=50)
            df['EMA_100'] = talib.EMA(df['close'],timeperiod=100)
            df['EMA_200'] = talib.EMA(df['close'],timeperiod=200)
            EMA_sub_var = [ ]
            for varx in range(0,len(EMA_list)):
                for vary in range(varx+1,len(EMA_list)):
                    df[EMA_list[varx]+"_"+EMA_list[vary]] = (abs(df[EMA_list[varx]] - df[EMA_list[vary]])/df['close'])*100
                    EMA_sub_var.append(EMA_list[varx]+"_"+EMA_list[vary])
            df['CLUSTER'] = df[EMA_sub_var].sum(axis=1)/len(EMA_sub_var)
            triple_cluster_rows = [k for k in EMA_sub_var if 'EMA_200' not in k]
            df['TRIPLE_CLUSTER'] = df[triple_cluster_rows].sum(axis=1)/len(triple_cluster_rows)
            df['U_BAND'],df['M_BAND'],df['L_BAND'] = talib.BBANDS(df['close'],timeperiod=14)
            df['Bands_Width'] = ((df['U_BAND']-df['L_BAND'])/df['M_BAND'])*100
            df['Bands_signal_SMA14'] = talib.SMA(df['Bands_Width'],timeperiod=14)
            df['Bands_signal_EMA10'] = talib.EMA(df['Bands_Width'],timeperiod=10)
            df['Bands_signal'] = (df['Bands_signal_SMA14'] + df['Bands_signal_EMA10'])/2
            candlsticks_dict = self.return_candlstick_data(df,self.CANDL_PATTRN,\
                            self.SINGL_CANDLSTICK_COUNT,self.MAX_CANDL_CANDLSTICKS,self.TOLERANCE)
            df['High_Low_300'] = (df.iloc[-300:]['high'].max() - df.iloc[-300:]['low'].min())/talib.SMA(df['close'],timeperiod=300)
            df['High_Low_150'] = (df.iloc[-150:]['high'].max() - df.iloc[-150:]['low'].min())/talib.SMA(df['close'],timeperiod=150)
            df['CLUSTER'] = df['CLUSTER']/df['High_Low_300']
            df['TRIPLE_CLUSTER'] = df['TRIPLE_CLUSTER']/df['High_Low_300']
            df['Bands_signal'] = df['Bands_signal']/df['High_Low_150']
            this_row = dict(zip(list(df.columns),list(df.iloc[-1].values)))
            this_row.update(candlsticks_dict)
            this_row_df = pd.DataFrame([this_row], columns=this_row.keys())
            if self.ALL_DF.empty :
                self.ALL_DF = pd.DataFrame(columns=this_row.keys())
            self.ALL_DF = pd.concat([self.ALL_DF,this_row_df],axis=0).reset_index(drop=True)

        return self.ALL_DF

    def save_df(self,df,BASE_PATH,telegram_column):
        f_time = datetime.datetime.strptime(df['date_time'].value_counts().index[0],'%Y-%m-%d %H:%M:%S')
        f_time_str = datetime.datetime.strftime(f_time,'%Y-%m-%d_%Hh%Mm')
        FILENAME = BASE_PATH + f_time_str + '.csv'
        df.to_csv(FILENAME,index=False)
        print(FILENAME)
        print("LOCAL_FILE_SAVED")
        if self.T_NOTIFY :
            telegram_msg = df[telegram_column].head(10).to_csv(index=False,sep=',')
            for trying in range(0,self.T_RETRY_COUNT) :
                if self.TeleGram_Notify(telegram_msg) :
                    break
                else :
                    sleep(self.T_RETRY_SLEEP)
        else:
            print("TELEGRAM NOTIFY is OFF")

    def save_signal(self,MODE):
        '''
        MODE = "CLUSTER","TRIPLE_CLUSTER","BB_BAND","SINGL_CANDLESTICKS"
        '''
        if self.ALL_DF.empty :
            self.ALL_DF = self.gen_signals_df()
        if MODE == "CLUSTER":
            BASE_PATH = self.CLUSTER_PATH + "CLUSTER_"
            sig_df = self.ALL_DF.sort_values(by=["CLUSTER"])
            sig_df["CLUSTER"] = round(sig_df["CLUSTER"],3)
            self.save_df(sig_df,BASE_PATH,['symbol','date_time',"CLUSTER"])
        elif MODE == "TRIPLE_CLUSTER":
            BASE_PATH = self.TRIPLE_CLUSTER_PATH + "TRIPLE_CLUSTER_"
            sig_df = self.ALL_DF.sort_values(by=["TRIPLE_CLUSTER"])
            sig_df["TRIPLE_CLUSTER"] = round(sig_df["TRIPLE_CLUSTER"],3)
            self.save_df(sig_df,BASE_PATH,['symbol','date_time',"TRIPLE_CLUSTER"])
        elif MODE == "BB_BAND" :
            BASE_PATH = self.BB_BAND_PATH + "BB_BAND_"
            sig_df = self.ALL_DF.sort_values(by=['Bands_signal'])
            sig_df['Bands_signal'] = round(sig_df['Bands_signal'],3)
            self.save_df(sig_df,BASE_PATH,['symbol','date_time','Bands_signal'])
        else: # MODE == "SINGL_CANDLESTICKS":
            BASE_PATH = self.SINGL_CANDLSTICKS_PATH + "SINGL_CANDLSTICKS_"
            sig_df = self.ALL_DF[ (self.ALL_DF['SINGL_CANDLSTICKS'].isin(self.CANDL_PATTRN))
                                & (self.ALL_DF['CANDL_RATIO_22'] != False) ]
            if sig_df.empty:
                print("NO CANDLSTICKS ")
            else:
                sig_df = sig_df.sort_values(by='CANDL_RATIO_22',ascending=False)
                self.save_df(sig_df,BASE_PATH,['symbol','date_time','SINGL_CANDLSTICKS','CANDL_RATIO_22'])


# for time in settings.TESTING_TIMEPERIOD:
#     if settings.SAVE_SIGNAL_DICT.get(time) :
#         sig = Signals(time,telegram_notify=settings.TELEGRAM_NOTIFY_DICT.get(time))
#         sig.save_signal("TRIPLE_CLUSTER")
#         sig.save_signal("CLUSTER")
#         sig.save_signal("BB_BAND")
#         sig.save_signal("SINGL_CANDLESTICKS")
