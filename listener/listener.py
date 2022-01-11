import time
import json
import os
import sys
from mysql.connector import connect, Error

from binance import ThreadedWebsocketManager, threaded_stream

api_key = os.environ["binance_api_key"]
api_secret = os.environ["binance_api_secret"]
sql_password = os.environ["sql_password"]


# source: https://python-binance.readthedocs.io/en/latest/websockets.html
# прослушивание с биржи n ответов


def get_timestamp_ms_gtm0():
    return time.time_ns() // 1_000_000


class SocketStorage:
    def __init__(self, twm, exchange, symbol):
        self.type_of_data = None
        self.sqlite_select_query = None
        self.cursor = None
        self.db_connection = None
        self.db_name = None
        self.symbol = symbol
        self.cnt = 0
        self.twm = twm
        self.exchange = exchange
        self.capacity_for_db = 1  # после скольких инсертов делаем коммит, видимо всегда будет 1 и этот функционал надо будет выпилить
        self.count_of_insert = 0
        self.time_bucket_db = 3 * 60 * 60 * 1000  # -- как часто обновляем бд, всё в ms
        self.table_name = None
        self.current_time_for_table_name = None

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
        except Error as e:
            print(e)
            self.twm.stop()

    def create_table(self):
        for type_of_data in ["trade", "kline", "depthUpdate"]:
            self.cursor.execute(
                "CREATE TABLE IF NOT EXISTS "
                + "_".join([type_of_data, str(self.current_time_for_table_name)])
                + "(timestamp BIGINT, data TEXT);"
            )

    def close_connection_to_db(self):
        self.db_connection.commit()
        self.cursor.execute("""SELECT count(*) from """ + self.table_name)
        print("count rows : ", self.cursor.fetchone())
        self.cursor.close()

    def handle_socket_message(self, msg):
        # print(msg)
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
            " currenct delta time : ",
            receive_time - server_time,
        )
        if self.cnt == 0:
            if not self.connect_to_db():
                self.twm.stop()  # !
        if self.upd_table_time(server_time):
            self.upd_table_name()
            self.create_table()
        else:
            self.upd_table_name()

        print(self.cnt)
        self.cnt += 1
        self.count_of_insert += 1
        message = json.dumps(msg)  # перевожу dict в строку чтобы записать в бд
        message = message.replace('"', '\\"')
        self.cursor.execute(
            "INSERT INTO "
            + self.table_name
            + " (timestamp, data) VALUES ("
            + str(server_time)
            + ', "'
            + message
            + '");'
        )
        if self.count_of_insert == self.capacity_for_db:
            self.db_connection.commit()
            print("COMMIT")
            self.count_of_insert = 0
        # if self.cnt == 5000000:
        #    self.close_connection_to_db()
        #    self.twm.stop()


def main():
    symbol = "BNBBTC"
    if len(sys.argv) > 1:
        symbol = sys.argv[1]
    print("symbol : ", symbol)

    twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)
    twm.start()  # обязательно до старта потоков прослушивания

    streams = [
        symbol.lower() + "@trade",
        symbol.lower() + "@depth",
        symbol.lower() + "@kline_1m",
    ]
    twm.start_multiplex_socket(
        callback=SocketStorage(twm, "binance", symbol).handle_socket_message,
        streams=streams,
    )

    print(*streams)
    twm.join()
    print("End")


if __name__ == "__main__":
    main()
