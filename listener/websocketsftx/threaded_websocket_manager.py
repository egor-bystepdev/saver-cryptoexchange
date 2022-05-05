import threading
import time
from websocketsftx.client import FtxWebsocketClient
    
class FTXThreadedWebsocketManager:
    def __init__(self, data_types : list, sleep_time, symbol : str, api_key : str, api_secret : str):
        self.data_types = data_types
        self.sleep_time = sleep_time
        self.symbol = symbol
        self.api_key = api_key
        self.api_secret = api_secret
    
    def start(self, callback):
        self.threads = []
        for data_type in self.data_types:
            self.threads.append(threading.Thread(target=create_thread_for_stream,
                                                args=(self.symbol, data_type, callback, self.sleep_time, self.api_key, self.api_secret)))

        for thread in self.threads:
            thread.start()
    
    def join(self):
        for thread in self.threads:
            thread.join()

    def stop(self):
        for thread in self.threads:
            thread.join()

def create_thread_for_stream(symbol, data_type, callback, sleep_time, api_key, api_secret):
    client = FtxWebsocketClient(api_key, api_secret)
    while True:
        if data_type == "trades":
            callback(client.get_trades(symbol), data_type)
        elif data_type == "orderbook":
            callback(client.get_orderbook(symbol), data_type)

        time.sleep(sleep_time)
