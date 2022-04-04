from concurrent.futures import thread
import json
import os
import sys
import threading
import time
import dateutil.parser

from binance import ThreadedWebsocketManager
from mysql.connector import connect, Error, errorcode
from websocketsftx.client import FtxWebsocketClient
from websocketsftx.threaded_websocket_manager import ThreadedWebsocketManagerFTX

api_key = os.environ["binance_api_key"]
api_secret = os.environ["binance_api_secret"]
sql_password = os.environ["sql_password"]


# source: https://python-binance.readthedocs.io/en/latest/websockets.html
# прослушивание с биржи n ответов


def get_timestamp_ms_gtm0():
    return time.time_ns() // 1_000_000


def isoformattotimestamp(server_time: str):
    return int(dateutil.parser.isoparse(server_time).timestamp() * 1000)


class SocketStorage:
    def __init__(self, twm, exchange, symbol, data_types):
        self.type_of_data = None
        self.sqlite_select_query = None
        self.cursor = None
        self.db_connection = None
        self.db_name = None
        self.symbol_original = symbol
        self.symbol = symbol.replace("-", "_")
        self.cnt = 0
        self.twm = twm
        self.exchange = exchange
        self.data_types = data_types
        self.capacity_for_db = 1  # после скольких инсертов делаем коммит, видимо всегда будет 1 и этот функционал
        # надо будет выпилить
        self.count_of_insert = 0
        self.time_bucket_db = 3 * 60 * 60 * 1000  # -- как часто обновляем бд, всё в ms
        self.table_name = None
        self.current_time_for_table_name = None
        self.sleep_time_for_ftx_websockets = 1500  # ms
        self.mutex = threading.Lock()

    def upd_db_name(self):
        self.db_name = self.exchange + "_" + self.symbol

    def upd_table_time(self, server_time):
        if self.table_name is None:
            self.current_time_for_table_name = (
                server_time - server_time % self.time_bucket_db
            )
            return True
        else:
            if server_time >= self.current_time_for_table_name + self.time_bucket_db:
                self.current_time_for_table_name += self.time_bucket_db
                return True
        return False

    def upd_table_name(self):
        self.table_name = (
            self.type_of_data + "_" + str(self.current_time_for_table_name)
        )

    def connect_to_db(self):
        self.cursor = None
        self.db_connection = None
        try:
            self.upd_db_name()
            print(self.symbol, " connect", self.db_name)
            self.db_connection = connect(
                user="root", password=sql_password, host="127.0.0.1"
            )
            self.cursor = self.db_connection.cursor()
            self.cursor.execute("create database if not exists " + self.db_name)
            self.cursor.execute("use " + self.db_name)
            print("База данных создана и успешно подключена к MySQL")

            self.sqlite_select_query = "select version();"
            self.cursor.execute(self.sqlite_select_query)
            record = self.cursor.fetchall()
            print("Версия базы данных MySql: ", record)
            return True
        except Error as err:
            print("Ошибка в connect_to_db:\n\t")
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Неправильный пароль или пользователь: ", err)
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Базы данных не существует: ", err)
            else:
                print(err)
            self.twm.stop()
            exit(1)

    def create_table(self):
        for type_of_data in self.data_types:
            try:
                self.cursor.execute(
                    "CREATE TABLE IF NOT EXISTS "
                    + "_".join([type_of_data, str(self.current_time_for_table_name)])
                    + "(timestamp BIGINT, data TEXT);"
                )
            except Error as err:
                print("Ошибка в create_table:\n\t")
                if err.errno == Error.ProgrammingError:
                    print("Синтаксическая ошибка в SQL запросе: ", err)
                elif err.errno == errorcode.DatabaseError:
                    print("Ошибка с базой данных: ", err)
                else:
                    print(err)

    def close_connection_to_db(self):
        try:
            self.db_connection.commit()
            self.cursor.execute("""SELECT count(*) from """ + self.table_name)

            print("count rows : ", self.cursor.fetchone())
            self.cursor.close()
        except Error as err:
            print("Ошибка в close_connection_to_db:\n\t")
            if err.errno == errorcode.ProgrammingError:
                print("Синтаксическая ошибка в SQL запросе: ", err)
            elif err.errno == errorcode.DatabaseError:
                print("Ошибка с базой данных: ", err)
            else:
                print(err)

    def ftx_msg_handler(self, messages: list, type_of_data: str):
        self.mutex.acquire()
        try:
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
        finally:
            self.mutex.release()

    def handle_socket_message(self, msg: dict):
        receive_time = get_timestamp_ms_gtm0()
        msg = msg["data"]
        server_time = msg["E"]
        self.type_of_data = msg["e"]
        msg["receive_time"] = receive_time

        print(
            self.type_of_data,
            " -- receive time : ",
            receive_time,
            " server time : ",
            server_time,
            " current delta time : ",
            receive_time - server_time,
        )
        if self.cnt == 0:
            self.connect_to_db()

        if self.upd_table_time(server_time):
            self.upd_table_name()
            self.create_table()
        else:
            self.upd_table_name()

        print(self.cnt)
        self.cnt += 1
        self.count_of_insert += 1
        message = json.dumps(msg)  # перевожу dict в строку, чтобы записать в бд
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
            print("Ошибка в handle_socket_message:\n\t")
            if err.errno == errorcode.ProgrammingError:
                print("Синтаксическая ошибка в SQL запросе: ", err)
            elif err.errno == errorcode.IntegrityError:
                print("Проблема с записью ключей: ", err)
            elif err.errno == errorcode.DatabaseError:
                print("Ошибка с базой данных: ", err)
            else:
                print(err)

        if self.count_of_insert == self.capacity_for_db:
            self.db_connection.commit()
            print("COMMIT")
            self.count_of_insert = 0


def main():
    symbol = "BNBBTC"
    exchange = "binance"
    if len(sys.argv) > 1:
        symbol = sys.argv[1]
    if len(sys.argv) > 2:
        exchange = sys.argv[2]
    print("symbol : ", symbol)
    print("exchange : ", exchange)

    if exchange == "binance":
        twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)
        twm.start()  # обязательно до старта потоков прослушивания

        data_types = [
            "trade",
            "kline",
            "depthUpdate",
        ]
        streams = [symbol.lower() + data_type for data_type in ["@trade", "@kline_1m", "@depth"]]
        twm.start_multiplex_socket(
            callback=SocketStorage(twm, exchange, symbol, data_types).handle_socket_message,
            streams=streams,
        )

        print(*streams)
        twm.join()
        print("End")
    elif exchange == "ftx":
        data_types = [
            "trades",
            "orderbook",
        ]

        twm = ThreadedWebsocketManagerFTX(data_types, 2, symbol)

        twm.start(SocketStorage(None, exchange, symbol, data_types).ftx_msg_handler)

        print(*data_types)

        twm.join()
        print("End")


if __name__ == "__main__":
    main()
