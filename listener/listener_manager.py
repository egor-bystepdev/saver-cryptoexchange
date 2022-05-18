from tabnanny import check
from binance import ThreadedWebsocketManager
from websocketsftx.threaded_websocket_manager import (
    FTXThreadedWebsocketManager,
)
import os
import threading
import json

from binance import ThreadedWebsocketManager

from listener import SocketStorage
from listener import data_types
from utils.helpers import *
from utils.db_manager import *
from utils.storage_exception import *


class ListenerManager:
    def __init__(self) -> None:
        with open("listener/config.json", "r") as fd:
            self.config = json.load(fd)
        self.api_key_binance = os.environ["binance_api_key"]
        self.api_secret_binance = os.environ["binance_api_secret"]
        self.api_key_ftx = os.environ["ftx_api_key"]
        self.api_secret_ftx = os.environ["ftx_api_secret"]
        self.twm_binance = ThreadedWebsocketManager(
            api_key=self.api_key_binance, api_secret=self.api_secret_ftx
        )
        self.twm_ftx = FTXThreadedWebsocketManager(
            api_key=self.api_key_ftx,
            api_secret=self.api_secret_ftx,
        )
        self.twm_binance.start()
        self.logger = create_logger("ListenerManager")
        self.socket_counter = 1
        self.binance_symbol_info = {}
        self.ftx_symbol_info = {}
        self.lock = threading.Lock()
        SocketChecker(
            self,
            self.config["checker_cooldown_s"],
            self.config["fatal_time_for_socket_s"],
        ).start()
        for listening_pair in self.config["symbols_in_start"]:
            self.start_listing(listening_pair["exchange"], listening_pair["symbol"])

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
                        callback=storage.handle_socket_message, streams=streams
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

                    storage = SocketStorage(
                        exchange, symbol, data_types[exchange], self.socket_counter
                    )
                    self.socket_counter += 1
                    socket_name = self.twm_ftx.start(
                        callback=storage.ftx_msg_handler, symbol=symbol
                    )
                    self.ftx_symbol_info[symbol] = (storage, socket_name)
                    self.logger.info(
                        f"Start listening {symbol} from {exchange} exchange\n"
                    )

                    return (
                        True,
                        f"Start listening {symbol} from {exchange} exchange\n",
                    )
                else:
                    return (False, "Unknown exchange")
        except Exception as err:
            handle_error("start listening", err, self.logger)
            return (False, str(err))

    def stop_listening(self, exchange: str, symbol: str):
        try:
            with self.lock:
                if exchange == "binance":
                    if symbol not in self.binance_symbol_info:
                        return (False, "No symbol in listening")
                    self.binance_symbol_info[symbol][0].stoped = True
                    self.twm_binance.stop_socket(self.binance_symbol_info[symbol][1])
                    self.binance_symbol_info.pop(symbol)
                    self.logger.info(
                        f"Stop listening {symbol} from {exchange} exchange\n"
                    )
                    return (
                        True,
                        f"Stop listening {symbol} from {exchange} exchange\n",
                    )

                elif exchange == "ftx":
                    if symbol not in self.ftx_symbol_info:
                        return (False, "Non symbol in listening")
                    self.ftx_symbol_info[symbol][0].stoped = True
                    self.twm_ftx.stop(self.ftx_symbol_info[symbol][1])
                    self.ftx_symbol_info.pop(symbol)
                    self.logger.info(
                        f"Stop listening {symbol} from {exchange} exchange\n"
                    )
                    return (
                        True,
                        f"Stop listening {symbol} from {exchange} exchange\n",
                    )
                else:
                    return (False, "Unknown exchange")
        except Exception as err:
            handle_error("Stop listening", err, self.logger)
            return (False, str(err))

    def get_all_messages(
        self,
        exchange,
        symbol,
        timestamp1,
        timestamp2,
        timestamp_in_ms=False,
        data_types_=[],
    ):
        try:
            with self.lock:
                storage = None
                time_bucket_db = 0
                if exchange == "binance":
                    if symbol in self.binance_symbol_info:
                        storage = self.binance_symbol_info[symbol][0]
                elif exchange == "ftx":
                    if symbol in self.ftx_symbol_info:
                        storage = self.ftx_symbol_info[symbol][0]
                else:
                    return ("Unknown exchange", [])
                db = None
                err = StorageException()

                if storage is None:
                    db = DBManager(exchange, symbol, data_types[exchange], 1, err)
                    if not db.connect():
                        self.logger.info(err.error.get_error())
                        return (False, "db_error")
                    time_bucket_db = 3 * 60 * 60 * 1000
                else:
                    db = storage.database
                    time_bucket_db = storage.time_bucket_db

                return (
                    "",
                    db.get_all_messages(
                        time_bucket_db,
                        timestamp1,
                        timestamp2,
                        timestamp_in_ms,
                        data_types_,
                    ),
                )
        except Exception as err:
            handle_error("get_all_messages", err, self.logger)
            return (False, str(err))


class SocketChecker(threading.Thread):
    def __init__(self, manager: ListenerManager, timer, check_timer):
        threading.Thread.__init__(self)
        self.manager = manager
        self.timer = timer
        self.check_timer = check_timer

    def run(self):
        while True:
            time.sleep(self.check_timer)
            self.checker()

    def checker(self):
        for socket_info in self.GetBinanceSockets():
            socket = socket_info[1][0]
            symbol = socket_info[0]
            if (
                socket.last_update.get_value() + self.timer < get_timestamp_ms_gtm0()
                and not socket.stoped
            ):
                self.manager.logger.info(socket.error.get_error())

                self.manager.stop_listening("binance", symbol)
                self.manager.start_listing("binance", symbol)
                handle_error(
                    "socket checker",
                    "socket " + symbol + " binance was failed, restart",
                    self.manager.logger,
                )
            else:
                self.manager.logger.info("socket " + symbol + " binance is OK")

        for socket_info in self.GetFtxSockets():
            socket = socket_info[1][0]
            symbol = socket_info[0]
            if (
                socket.last_update.get_value() + self.timer < get_timestamp_ms_gtm0()
                and not socket.stoped
            ):
                print(socket.last_update.get_value())
                self.manager.logger.info(socket.error.get_error())

                self.manager.stop_listening("ftx", symbol)
                self.manager.start_listing("ftx", symbol)
                handle_error(
                    "socket checker",
                    "socket " + symbol + " ftx was failed, restart",
                    self.manager.logger,
                )
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


# exchange_data_types = {
# 	"binance": ["trade", "kline", "depthUpdate"],
# 	"ftx": ["trades", "orderbook"]
# }

# ls = ListenerManager()

# print(ls.get_all_messages("ftx", "BTC/USDT", get_timestamp_ms_gtm0() - 100000, get_timestamp_ms_gtm0(), True, exchange_data_types["ftx"]))

# ls.start_listing("ftx", "BTC/USDT")

# time.sleep(20)

# ls.stop_listening("ftx", "BTC/USDT")
# print(ls.get_all_messages("ftx", "BTC/USDT", get_timestamp_ms_gtm0() - 10000, get_timestamp_ms_gtm0(), True, exchange_data_types["ftx"]))

# ls.start_listing("binance", "BNBBTC")

# time.sleep(10)
# ptr = 0
# while (ptr < 3):
#     print("STOP")
#     time.sleep(5)
#     print(ls.get_all_messages("ftx", "BTC/USDT", get_timestamp_ms_gtm0() - 100000, get_timestamp_ms_gtm0(), True, exchange_data_types["ftx"]))
#     ptr += 1

# ls.stop_listening("ftx", "BTC/USDT")

# time.sleep(10)

# ls.stop_listening("binance", "BNBBTC")
