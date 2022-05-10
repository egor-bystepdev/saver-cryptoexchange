from tabnanny import check
from binance import ThreadedWebsocketManager
from websocketsftx.threaded_websocket_manager import FTXThreadedWebsocketManager
import os
import logging as log
import time
from utils.helpers import *
from listener import SocketStorage
from listener import data_types
import threading


class ListenerManager:
    def __init__(self) -> None:
        self.api_key_binance = os.environ["binance_api_key"]
        self.api_secret_binance = os.environ["binance_api_secret"]
        self.api_key_ftx = os.environ["ftx_api_key"]
        self.api_secret_ftx = os.environ["ftx_api_secret"]
        self.twm_binance = ThreadedWebsocketManager(
            api_key=self.api_key_binance, api_secret=self.api_secret_ftx
        )
        self.twm_binance.start()
        self.logger = create_logger("ListenerManager")
        self.socket_counter = 1
        self.binance_symbol_info = {}
        self.ftx_symbol_info = {}
        self.lock = threading.Lock()
        SocketChecker(self).start()

    def start_listing(self, exchange: str, symbol: str):
        try:
            with self.lock:
                if exchange == "binance":
                    if symbol in self.binance_symbol_info:
                        return (False, "Symbol already in listening")

                    storage = SocketStorage(
                        exchange, symbol, data_types[exchange], self.socket_counter
                    )
                    self.socket_counter += 1
                    streams = [
                        symbol.lower() + data_type
                        for data_type in ["@trade", "@kline_1m", "@depth"]
                    ]
                    socket_name = self.twm_binance.start_multiplex_socket(
                        callback=storage.handle_socket_message,
                        streams=streams,
                    )  # check try except
                    self.binance_symbol_info[symbol] = (storage, socket_name)
                    self.logger.info(
                        f"Start listening {symbol} from {exchange} exchange\n"
                    )

                    return (
                        True,
                        f"Start listening {symbol} from {exchange} exchange\n",
                    )
                elif exchange == "ftx":
                    if symbol in self.ftx_symbol_info:
                        return (False, "Symbol already in listening")
                    
                    twm = FTXThreadedWebsocketManager(
                        api_key=self.api_key_ftx,
                        api_secret=self.api_secret_ftx,
                    )
                    storage = SocketStorage(
                        exchange, symbol, data_types[exchange], self.socket_counter
                    )
                    self.socket_counter += 1
                    twm.start(storage.ftx_msg_handler, symbol=symbol)
                    self.ftx_symbol_info[symbol] = (storage, twm)
                    print(len(self.ftx_symbol_info))
                    return (
                        True,
                        f"Start listening {symbol} from {exchange} exchange\n",
                    )
                else:
                    return (False, "Unknown exchange")
        except Exception as err:
            handle_error("start listening", err, self.logger)
            return (False, str(err))

    def stop_listening(self, exchange: str, symbol: str): # удалить их из списка
        try:
            with self.lock:
                if exchange == "binance":
                    if symbol not in self.binance_symbol_info:
                        return (False, "No symbol in listening")
                    self.binance_symbol_info[symbol][0].stoped = True
                    self.twm_binance.stop_socket(self.binance_symbol_info[symbol][1])
                    self.binance_symbol_info.pop(symbol)
                    return (
                        True,
                        f"Stop listening {symbol} from {exchange} exchange\n",
                    )

                elif exchange == "ftx":
                    if symbol not in self.ftx_symbol_info:
                        return (False, "Non symbol in listening")
                    self.ftx_symbol_info[symbol][0].stoped = True
                    self.ftx_symbol_info[symbol][1].stop(symbol)
                    self.ftx_symbol_info.pop(symbol)
                    return (
                        True,
                        f"Stop listening {symbol} from {exchange} exchange\n",
                    )
                else:
                    return (False, "Unknown exchange")
        except Exception as err:
            handle_error("stop listening", err, self.logger)
            return (False, str(err))

    def get_all_messages(
        self,
        exchange,
        symbol,
        timestamp1,
        timestamp2,
        timestamp_in_ms=False,
        data_types=[],
    ):
        try:
            with self.lock:
                if exchange == "binance":
                    if not symbol in self.binance_symbol_info:
                        return ("symbol not in listening", [])
                    storage = self.binance_symbol_info[symbol][0]
                    return (
                        "",
                        storage.database.get_all_messages(
                            storage.time_bucket_db,
                            timestamp1,
                            timestamp2,
                            timestamp_in_ms,
                            data_types,
                        ),
                    )
                elif exchange == "ftx":
                    if not symbol in self.binance_symbol_info:
                        return ("symbol not in listening", [])
                    storage = self.ftx_symbol_info[symbol][0]
                    return (
                        "",
                        storage.database.get_all_messages(
                            storage.time_bucket_db,
                            timestamp1,
                            timestamp2,
                            timestamp_in_ms,
                            data_types,
                        ),
                    )
                else:
                    return ("Unknown exchange", [])
        except Exception as err:
            handle_error("get_all_messages", err, self.logger)
            return (False, str(err))


class SocketChecker(threading.Thread):
    def __init__(self, manager : ListenerManager):
        threading.Thread.__init__(self)
        self.manager = manager
        self.timer = 1000 * 60 * 5
        self.check_timer = 1000 * 60 * 10

    def run(self):
       while (True):
           time.sleep(self.check_timer)
           self.checker()
        
    def checker(self):
        for socket_info in self.GetBinanceSockets():
            socket = socket_info[1][0]
            symbol = socket_info[0]
            if (socket.last_update.get_value() + self.timer < get_timestamp_ms_gtm0() and not socket.stoped):
                self.manager.start_listing("binance", symbol)
                self.manager.start_listing("binance", symbol)
                handle_error("socket checker", "socket " + symbol + " binance was failed, restart", self.manager.logger)
            else:
                self.manager.logger.info("socket " + symbol + " binance is OK")
        
        for socket_info in self.GetFtxSockets():
            socket = socket_info[1][0]
            symbol = socket_info[0]
            if (socket.last_update.get_value() + self.timer < get_timestamp_ms_gtm0() and not socket.stoped):
                self.manager.start_listing("ftx", symbol)
                self.manager.start_listing("ftx", symbol) 
                handle_error("socket checker", "socket " + symbol + " ftx was failed, restart", self.manager.logger)
            else:
                self.manager.logger.info("socket " + symbol + " ftx is OK")
    
    def GetBinanceSockets(self):
        list_of_sockets = []
        with self.manager.lock:
            for key, value in self.manager.binance_symbol_info.items():
                list_of_sockets.append((key, value))
        
        return list_of_sockets
    
    def GetFtxSockets(self):
        list_of_sockets = []
        with self.manager.lock:
            for key, value in self.manager.ftx_symbol_info.items():
                list_of_sockets.append((key, value))
        
        return list_of_sockets


'''

exchange_data_types = {
	"binance": ["trade", "kline", "depthUpdate"],
	"ftx": ["trades", "orderbook"]
}

ls = ListenerManager()

ls.start_listing("ftx", "NEAR_USDT")

time.sleep(20)

ls.stop_listening("ftx", "NEAR_USDT")

while (True):
    print("STOP")
    time.sleep(5)
    # print(ls.get_all_messages("binance", "BNBBTC", get_timestamp_ms_gtm0() - 100000, get_timestamp_ms_gtm0(), True, exchange_data_types["binance"]))
    
'''

