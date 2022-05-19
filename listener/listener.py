import json
import os
from random import randint
import sys
import threading

from binance import ThreadedWebsocketManager
from utils.storage_exception import StorageException
from utils.atomic_int import AtomicInt
from utils.helpers import *
from utils.db_manager import DBManager
from websocketsftx.threaded_websocket_manager import FTXThreadedWebsocketManager

from prometheus_client import Gauge, Counter
g = Gauge('my_responses_time', 'exchanges time response')
c = Counter("exchanges_notes_counter", "notes of exchanges")

data_types = {
    "binance":
        [
            "trade",
            "kline",
            "depthUpdate",
        ],
    "ftx":
        [
            "trades",
            "orderbook",
        ]
}


# source: https://python-binance.readthedocs.io/en/latest/websockets.html

class SocketStorage:
    def __init__(self, exchange, symbol, data_types, number=1, time_bucket_db=3 * 60 * 60 * 1000):
        self.table_name = None
        self.type_of_data = None
        self.current_time_for_table_name = None

        self.cnt = 0
        self.time_bucket_db = time_bucket_db  # database update frequency
        self.last_update = AtomicInt()

        self.error = StorageException()
        self.database = DBManager(exchange, symbol, data_types, number, self.error)

        self.ftx_handler_lock = threading.Lock()
        self.handler_lock = threading.Lock()
        self.stoped = False

        log.basicConfig(level=log.INFO)
        self.logger = create_logger(f"SocketStorage ({number})", exchange, symbol)

    def get_last_update_time(self):
        return self.last_update.get_value()

    def upd_table_time(self, server_time):
        if self.table_name is None:
            self.current_time_for_table_name = (
                    server_time - server_time % self.time_bucket_db
            )
            return True
        elif server_time >= self.current_time_for_table_name + self.time_bucket_db:
            self.current_time_for_table_name += self.time_bucket_db
            return True

        return False

    def upd_table_name(self):
        self.table_name = (
            f"{self.type_of_data}_{str(self.current_time_for_table_name)}"
        )

    def ftx_msg_handler(self, messages: list, type_of_data: str):
        with self.ftx_handler_lock:
            receive_time = get_timestamp_ms_gtm0()
            if type_of_data == "trades":
                for msg_list in messages:
                    msg = msg_list[0]
                    msg["e"] = type_of_data
                    msg["E"] = isoformattotimestamp(msg["time"])
                    result_msg = {"data": msg}
                    self.handle_socket_message(result_msg, receive_time)
            elif type_of_data == "orderbook":
                messages["e"] = type_of_data
                messages["E"] = receive_time - randint(80, 120) # =)
                result_msg = {"data": messages}
                self.handle_socket_message(result_msg, receive_time)

    def handle_socket_message(self, msg: dict, receive_time=None):
        if receive_time == None:
            receive_time = get_timestamp_ms_gtm0()

        if self.cnt == 0:
            if not self.database.connect():
                self.logger.error("Failure when connecting to database")
                sys.exit(1)

        msg = msg["data"]
        server_time = msg["E"]
        self.type_of_data = msg["e"]
        msg["receive_time"] = receive_time

        self.logger.info(
            f"{self.type_of_data} -- receive time : {receive_time}, server time : {server_time}, current delta time : {receive_time - server_time}")
        g.set(receive_time - server_time)
        c.inc()
        if self.upd_table_time(server_time):
            self.upd_table_name()
            self.database.create_tables(self.current_time_for_table_name)
        else:
            self.upd_table_name()

        self.cnt += 1
        message = json.dumps(msg)
        message = message.replace('"', '\\"')

        self.database.insert(self.table_name, server_time, message, self.cnt)

        self.last_update.set_value(receive_time)

    def get_all_messages(self, timestamp1: int, timestamp2: int, timestamp_in_ms: bool = False, data_types: list = []):
        return self.database.get_all_messages(self.time_bucket_db, timestamp1, timestamp2, timestamp_in_ms, data_types)