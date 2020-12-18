import os
import struct
import time
import pandas as pd
class DWX_MT4_HISTORY():

    def __init__(self):

        self._HST_BYTE_FORMAT = '<Qddddqiq'
        self._HEADER_BYTES = 148
        self._BAR_BYTES = 60

    ##########################################################################

    def get_history(self, _filename,_symbol,_timeframe,_bars,_verbose):

        if _filename == None:
            print('[ERROR] Invalid filename!')
            quit()

        _seek = 0
        _open_time = []
        _open_price = []
        _low_price = []
        _high_price = []
        _close_price = []
        _volume = []
        _spread = []
        _real_volume = []

        COLUMNS = ['date_time','open','high','low','close','volume']
        _df = pd.DataFrame(columns=COLUMNS)
        try:
            with open(_filename.format(_symbol, _timeframe), 'rb') as f:

                _filesize = os.stat(_filename.format(_symbol, _timeframe)).st_size
                _num_bars = int((_filesize - self._HEADER_BYTES) / self._BAR_BYTES)
                if _bars == -1 :
                    _seek_bars = 0
                else:
                    _seek_bars = max(_num_bars - _bars,0)

                ## Read Header
                _buf = f.read(self._HEADER_BYTES)

                ## Move at seek
                f.seek(self._HEADER_BYTES + _seek_bars*self._BAR_BYTES)

                # Read OHLC starting from seek
                for count_bar in range(0,_num_bars-_seek_bars):
                    _buf = f.read(self._BAR_BYTES)
                    _bar = struct.unpack(self._HST_BYTE_FORMAT, _buf)
                    _open_time.append(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(_bar[0])))
                    _open_price.append(_bar[1])
                    _high_price.append(_bar[2])
                    _low_price.append(_bar[3])
                    _close_price.append(_bar[4])
                    _volume.append(_bar[5])
                    _spread.append(_bar[6])
                    _real_volume.append(_bar[7])

        except:
            return _df

        _df = pd.DataFrame.from_dict(
                {'date_time': _open_time,
                 'open': _open_price,
                 'high': _high_price,
                 'low': _low_price,
                 'close': _close_price,
                 'volume': _volume})


        if _verbose is True:
            print(_df)

        return _df


# FILE_PATH = "C://Users//raghu//AppData//Roaming//MetaQuotes//Terminal//50CA3DFB510CC5A8F28B48D1BF2A5702//history//Tradize-Demo//"
# FILE_PATH2 = "C://Users//raghu//AppData//Roaming//MetaQuotes//Terminal//50CA3DFB510CC5A8F28B48D1BF2A5702//history//SimpleFintech-Demo//"
# SYMBOL = "NIITTECH"
# TIMEFRAME = '60'
# FILENAME = FILE_PATH2 + SYMBOL + TIMEFRAME + '.hst'
# VERBOSE = False
# LAST_BAR = -1
#
# hist_data = DWX_MT4_HISTORY()
# df = hist_data.get_history(_filename=FILENAME,_symbol=SYMBOL,_timeframe=TIMEFRAME, _bars= LAST_BAR, _verbose=False)
# print(df.head())
# print(df.tail())
# print(len(df))
# if df.empty:
#     print("EMOTY DF FOUND")
