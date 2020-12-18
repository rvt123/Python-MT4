import zmq
import pandas as pd
from pandas import Timestamp
from time import sleep
import settings
DF_COLUMNS = ['symbol','date_time','open','high','low','close','volume']

class DWX_ZeroMQ_Connector():
    """
    Setup ZeroMQ -> MetaTrader Connector
    """
    def __init__(self,_PUSH_PORT,_PULL_PORT,
                _protocol='tcp',
                 _host='localhost'):

        # ZeroMQ Context
        self._ZMQ_CONTEXT = zmq.Context()
        # Connection Protocol
        self._protocol = _protocol
        # ZeroMQ Host
        self._host = _host
        # TCP Connection URL Template
        self._URL = self._protocol + "://" + self._host + ":"
        # Ports for PUSH, PULL and SUB sockets respectively
        self._PUSH_PORT = _PUSH_PORT
        self._PULL_PORT = _PULL_PORT
        # Create Sockets
        self._PUSH_SOCKET = self._ZMQ_CONTEXT.socket(zmq.PUSH)
        self._PULL_SOCKET = self._ZMQ_CONTEXT.socket(zmq.PULL)
        # Bind PUSH Socket to send commands to MetaTrader
        self._PUSH_SOCKET.connect(self._URL + str(self._PUSH_PORT))
        print("[INIT] Ready to send commands to METATRADER (PUSH): " + str(self._PUSH_PORT))
        # print("URL is ",self._URL + str(self._PUSH_PORT))
        # Connect PULL Socket to receive command responses from MetaTrader
        self._PULL_SOCKET.connect(self._URL + str(self._PULL_PORT))
        print("[INIT] Listening for responses from METATRADER (PULL): " + str(self._PULL_PORT))
        # print("URL is ",self._URL + str(self._PULL_PORT))
        self.history_df = pd.DataFrame(columns=DF_COLUMNS)
    ##########################################################################

    def remote_send(self, socket, data):
        try:
            socket.send_string(data)
            # print ("Data send is ",data)
        except zmq.Again as e:
            # print ("Waiting for PUSH from MetaTrader 4..")
            pass
    def remote_pull(self, socket):
        try:
            msg_pull = socket.recv(flags=zmq.NOBLOCK)
            return msg_pull
        except zmq.Again as e:
            # print ("Waiting for PUSH from MetaTrader 4..")
            pass

    """
    Function to construct messages for sending DATA commands to MetaTrader
    """
    def _DWX_MTX_SEND_MARKETDATA_REQUEST_(self,
                                 _symbol,
                                 _timeframe,
                                 _start,
                                 _end):

        _msg = "{};{};{};{};{}".format('DATA',
                                     _symbol,
                                     _timeframe,
                                     _start,
                                     _end)
        # Send via PUSH Socket
        # print("Msg send is ",_msg)
        self.remote_send(self._PUSH_SOCKET, _msg)
        sleep(3) ## Time to send the command and wait for data to be ready
        _data = self.remote_pull(self._PULL_SOCKET)
        for count in range(0,3):
            if _data is None :
                self.remote_send(self._PUSH_SOCKET, _msg)
                sleep(1+(count/2))
                _data = self.remote_pull(self._PULL_SOCKET)
            else:
                break
        if _data is None:
            print("SOMETHING's FISHY! PLS CHECK")
            return(False)
        else:
            self.history_df = pd.DataFrame(columns=DF_COLUMNS)
            data_dict = eval(_data)
            data_rows = data_dict['DATA'].split('|')
            data_rows = data_rows[:-1]
            for row in data_rows:
                row_data = row.split(';')
                this_row = dict(zip(DF_COLUMNS,row_data))
                this_row_df = pd.DataFrame([this_row], columns=this_row.keys())
                self.history_df = pd.concat([self.history_df,this_row_df],axis=0).reset_index(drop=True)
            self.history_df = self.history_df.sort_values(by='date_time').reset_index(drop=True)
            self.history_df = self.history_df.drop_duplicates().reset_index(drop=True)
            return self.history_df


    ##########################################################################
