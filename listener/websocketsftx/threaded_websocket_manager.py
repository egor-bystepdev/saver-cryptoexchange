import threading
import time
from websocketsftx.client import FtxWebsocketClient
from utils.helpers import create_logger
    
class FTXThreadedWebsocketManager:
    def __init__(self, api_key : str, api_secret : str, number=1, sleep_time=1):
        self.data_types = ["trades", "orderbook"]
        self.sleep_time = sleep_time
        self.api_key = api_key
        self.api_secret = api_secret

        self.threads = {}
        self.clients = {}

        self.log = create_logger(f"FTX TWM ({number})")
    
    def start(self, callback, symbol):
        self.threads[symbol] = []
        self.clients[symbol] = {
            "trades": FtxWebsocketClient(self.api_key, self.api_secret),
            "orderbook": FtxWebsocketClient(self.api_key, self.api_secret),
        }

        for data_type in self.data_types:
            self.threads[symbol].append(threading.Thread(target=create_thread_for_stream,
                                                args=(self.clients[symbol], symbol, data_type, callback, self.sleep_time)))

        type = 0
        for thread in self.threads[symbol]:
            self.log.info(f"start listening data type ({self.data_types[type]}) for symbol ({symbol})\n")
            thread.start()
            type += 1
    
    def join(self):
        for symbol in self.threads:
            for thread in self.threads[symbol]:
                thread.join()
    
    def stop_all(self):
        for symbol in self.threads:
            for data_type in self.data_types:
                self.clients[symbol][data_type].unsubscribe(symbol, data_type)

            for thread in self.threads[symbol]:
                thread.join()
 
    def stop(self, symbol):
        if symbol not in self.threads:
            self.log.info(f"symbol ({symbol}) is not listened at this moment")
            return

        for data_type in self.data_types:
            self.clients[symbol][data_type].unsubscribe(symbol, data_type)
        del self.clients[symbol]

        for thread in self.threads[symbol]:
            thread.join()
        del self.threads[symbol]

def create_thread_for_stream(clients, symbol, data_type, callback, sleep_time):
    while True:
        if clients[data_type].stopped():
            break

        if data_type == "trades":
            callback(clients[data_type].get_trades(symbol), data_type)
        elif data_type == "orderbook":
            callback(clients[data_type].get_orderbook(symbol), data_type)

        time.sleep(sleep_time)
