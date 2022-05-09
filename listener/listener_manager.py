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
                    twm = FTXThreadedWebsocketManager(
                        data_types[exchange],
                        1,
                        symbol,
                        self.api_key_ftx,
                        self.api_secret_ftx,
                    )
                    storage = SocketStorage(
                        exchange, symbol, data_types[exchange], self.socket_counter
                    )
                    self.socket_counter += 1
                    twm.start(storage.ftx_msg_handler)
                    self.ftx_symbol_info[symbol] = (storage, twm)
                else:
                    return (False, "Unknown exchange")
        except Exception as err:
            handle_error("start listening", err, self.logger)
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


"""
exchange_data_types = {
	"binance": ["trade", "kline", "depthUpdate"],
	"ftx": ["trades", "orderbook"]
}

ls = ListenerManager()

ls.start_listing("binance", "BNBBTC")

while (True):
    time.sleep(5)
    print(ls.get_all_messages("binance", "BNBBTC", get_timestamp_ms_gtm0() - 100000, get_timestamp_ms_gtm0(), True, exchange_data_types["binance"]))
    print

"""
