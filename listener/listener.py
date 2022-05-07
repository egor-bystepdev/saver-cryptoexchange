import json
import os
import sys
import threading
import time
import dateutil.parser
import logging as log

from binance import ThreadedWebsocketManager
from mysql.connector import connect, Error, errorcode
from utils.atomic_int import AtomicInt
from websocketsftx.client import FtxWebsocketClient
from websocketsftx.threaded_websocket_manager import FTXThreadedWebsocketManager

sql_password = os.environ["sql_password"]

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

def get_timestamp_ms_gtm0():
    return time.time_ns() // 1_000_000


def isoformattotimestamp(server_time: str):
    return int(dateutil.parser.isoparse(server_time).timestamp() * 1000)


def handle_error(function_name, err, logger):
    logger.error(f"Error in {function_name}")
    logger.error(err)
    os._exit(1)

class SocketStorage:
    def __init__(self, exchange, symbol, data_types):
        self.type_of_data = None
        self.cursor = None
        self.db_connection = None
        self.db_name = None
        self.table_name = None
        self.current_time_for_table_name = None

        self.symbol = symbol.replace("-", "_")
        self.cnt = 0
        self.exchange = exchange
        self.data_types = data_types
        self.time_bucket_db = 3 * 60 * 60 * 1000  #database update frequency
        self.last_update = AtomicInt()

        self.ftx_handler_lock = threading.Lock()
        self.handler_lock = threading.Lock()
        
        log.basicConfig(format="[SocketStorage %(levelname)s]: %(message)s", level=log.INFO)
    
    def get_last_update_time(self):
        return self.last_update.get_value()

    def upd_db_name(self):
        self.db_name = f"{self.exchange}_{self.symbol}"

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

    def connect_to_db(self):
        self.cursor = None
        self.db_connection = None
        try:
            self.upd_db_name()

            log.info(f"{self.symbol} connecting to {self.db_name}...\n")
            self.db_connection = connect(
                user="root", password=sql_password, host="127.0.0.1"
            )

            self.cursor = self.db_connection.cursor()
            self.cursor.execute("CREATE DATABASE IF NOT EXISTS " + self.db_name)
            self.cursor.execute("USE " + self.db_name)
            log.info("Database created and succesfully connected to MySQL")

            self.cursor.execute("SELECT VERSION()")
            record = self.cursor.fetchall()
            log.info(f"Database version: {record[0][0]}\n")

            return True
        except Error as err:
            handle_error("connect_to_db", err, log)

    def create_table(self):
        for type_of_data in self.data_types:
            try:
                self.cursor.execute(
                    "CREATE TABLE IF NOT EXISTS "
                    + "_".join([type_of_data, str(self.current_time_for_table_name)])
                    + "(timestamp BIGINT, data TEXT);"
                )
            except Error as err:
                handle_error("create_table", err, log)

    def close_connection_to_db(self):
        try:
            self.db_connection.commit()
            self.cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")

            log.info(f"count rows : {self.cursor.fetchone()}")
            self.cursor.close()
        except Error as err:
            handle_error("close_connection_to_db", err, log)

    def ftx_msg_handler(self, messages: list, type_of_data: str):
        with self.ftx_handler_lock:
            receive_time = get_timestamp_ms_gtm0()
            if type_of_data == "trades":
                for msg_list in messages:
                    msg = msg_list[0]
                    msg["e"] = type_of_data
                    msg["E"] = isoformattotimestamp(msg["time"])
                    result_msg = {"data": msg}
                    self.handle_socket_message(result_msg)
            elif type_of_data == "orderbook":
                messages["e"] = type_of_data
                messages["E"] = receive_time
                result_msg = {"data": messages}
                self.handle_socket_message(result_msg)

    def handle_socket_message(self, msg: dict):
        receive_time = get_timestamp_ms_gtm0()

        if self.cnt == 0:
            self.connect_to_db()

        msg = msg["data"]
        server_time = msg["E"]
        self.type_of_data = msg["e"]
        msg["receive_time"] = receive_time

        log.info(f"{self.type_of_data} -- receive time : {receive_time}, server time : {server_time}, current delta time : {receive_time - server_time}")

        if self.upd_table_time(server_time):
            self.upd_table_name()
            self.create_table()
        else:
            self.upd_table_name()

        self.cnt += 1
        message = json.dumps(msg)
        message = message.replace('"', '\\"')

        try:
            self.cursor.execute(
                "INSERT INTO "
                + self.table_name
                + " (timestamp, data) VALUES ("
                + str(server_time)
                + ', "'
                + message
                + '");'
            )
        except Error as err:
            handle_error("handle_socket_message", err, log)

        self.db_connection.commit()
        log.info(f"{self.cnt} COMMIT\n")
        self.last_update.set_value(receive_time)
        
    def get_all_messages(self, timestamp1: int, timestamp2: int, timestamp_in_ms: bool = False, data_type: list = []):
        if timestamp1 > timestamp2:
            return []

        try:
            if not timestamp_in_ms:
                timestamp1 *= 1000
                timestamp2 *= 1000
            start_timestamp = timestamp1 - timestamp1 % self.time_bucket_db
            finish_timestamp = timestamp2 + (self.time_bucket_db - timestamp2 % self.time_bucket_db)

            result = []
            for type_of_data in data_types:
                for timestamp in range(start_timestamp, finish_timestamp, self.time_bucket_db):
                    table_name = "_".join([type_of_data, str(timestamp)])

                    query = f"SELECT * FROM {table_name} WHERE timestamp >= {timestamp1} AND timestamp <= {timestamp2};"
                    log.info(f"QUERY: {query}")

                    self.cursor.execute(query)
                    result += self.cursor.fetchall()

            return result
        except Error as err:
            log.error("Error in get_all_messages")
            log.error(err)

            return []

def main_log(info):
    print(f"[Listener INFO]: {info}")

def main():
    symbol = "BNBBTC"
    exchange = "binance"
    if len(sys.argv) > 1:
        symbol = sys.argv[1]
    if len(sys.argv) > 2:
        exchange = sys.argv[2]

    main_log(f"symbol : {symbol}")
    main_log(f"exchange : {exchange}\n")

    api_key = os.environ[exchange + "_api_key"]
    api_secret = os.environ[exchange + "_api_secret"]

    storage = SocketStorage(exchange, symbol, data_types[exchange])

    if exchange == "binance":
        twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)
        twm.start()

        streams = [symbol.lower() + data_type for data_type in ["@trade", "@kline_1m", "@depth"]]
        twm.start_multiplex_socket(
            callback=storage.handle_socket_message,
            streams=streams,
        )

        main_log(f"listening {', '.join(streams)} from {exchange} exchange\n")
        twm.join()
    elif exchange == "ftx":
        twm = FTXThreadedWebsocketManager(data_types[exchange], 1, symbol, api_key, api_secret)

        twm.start(storage.ftx_msg_handler)

        main_log(f"listening {', '.join(data_types[exchange])} from {exchange} exchange\n")
        twm.join()
    else:
        print(f"[Listener ERROR]: No such exchange {exchange}")
        os._exit(1)


if __name__ == "__main__":
    main()