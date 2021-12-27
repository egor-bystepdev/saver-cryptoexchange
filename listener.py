import time
import sqlite3
import json
import os
import sys

from binance import ThreadedWebsocketManager

api_key = os.environ["binance_api_key"]
api_secret = os.environ["binance_api_secret"]


# source: https://python-binance.readthedocs.io/en/latest/websockets.html
# прослушивание с биржи n ответов


def get_timestamp_ms_gtm0():
    return time.time_ns() // 1_000_000


def create_dir(dir_name):
    try:
        os.mkdir(dir_name)
        print("create dir : ", dir_name)
    except FileExistsError:
        print("dir already exists : ", dir_name)


class SocketStorage:
    def __init__(self, name, twm, exchange):
        self.stream_name = name
        self.cnt = 0
        self.twm = twm
        self.exchange = exchange
        self.capacity_for_db = 1000  # после скольких инсертов делаем коммит
        self.count_of_insert = 0
        self.time_bucket_db = 3 * 60 * 60 * 1000  # -- как часто обновляем бд, всё в ms
        self.table_name = None
        self.current_time_for_table_name = None

    def upd_db_path(self):
        create_dir(self.exchange)
        self.db_path = self.exchange + "/" + self.type_of_data

    def upd_table_name(self, server_time):
        if self.table_name == None:
            self.table_name = "data_" + str(
                server_time - server_time % self.time_bucket_db
            )
            self.current_time_for_table_name = (
                server_time - server_time % self.time_bucket_db
            )
            return True
        else:
            if server_time >= self.current_time_for_table_name + self.time_bucket_db:
                self.current_time_for_table_name += self.time_bucket_db
                self.table_name = "data_" + str(self.current_time_for_table_name)
                return True
        return False

    def connect_to_db(self):
        self.cursor = None
        self.sqlite_connection = None
        try:
            self.upd_db_path()
            print(self.type_of_data, " connect", self.db_path)
            self.sqlite_connection = sqlite3.connect(self.db_path + ".db")
            self.cursor = self.sqlite_connection.cursor()
            print("База данных создана и успешно подключена к SQLite")

            self.sqlite_select_query = "select sqlite_version();"
            self.cursor.execute(self.sqlite_select_query)
            record = self.cursor.fetchall()
            print("Версия базы данных SQLite: ", record)
            return True
        except sqlite3.Error as error:
            print("Ошибка при подключении к sqlite", error)
            return False

    def create_table(self):
        self.cursor.execute(
            """ SELECT count(name) FROM sqlite_master WHERE type='table' AND name='"""
            + self.table_name
            + """' """
        )
        if self.cursor.fetchone()[0] != 1:  # чек что такой таблицы ещё нет
            print("Create table in db :", self.db_path, " with name :", self.table_name)
            self.cursor.execute(
                """CREATE TABLE """
                + self.table_name
                + """
                        (time int, msg text)
                """
            )

    def close_connection_to_db(self):
        self.sqlite_connection.commit()
        self.cursor.execute("""SELECT count(*) from """ + self.table_name)
        print("count rows : ", self.cursor.fetchone())
        # если захочется посмотреть данные, то
        # curcor.execute("""select * from depth""")
        # print(cursor.fetchall())
        self.cursor.close()

    def handle_socket_message(self, msg):
        receive_time = get_timestamp_ms_gtm0()
        server_time = msg["E"]

        print("receive time : ", get_timestamp_ms_gtm0(), " server time : ", msg["E"])
        # print(msg)
        if self.cnt == 0:
            self.type_of_data = msg["e"]
            if self.connect_to_db() == False:
                self.twm.stop()  # !
        if self.upd_table_name(server_time):
            self.create_table()

        print(self.cnt)
        self.cnt += 1
        self.count_of_insert += 1
        message = json.dumps(msg)  # перевожу dict в строку чтобы записать в бд
        self.cursor.executemany(
            "INSERT INTO " + self.table_name + " (time, msg) VALUES (?,?)",
            [(server_time, message)],
        )
        if self.count_of_insert == self.capacity_for_db:
            self.sqlite_connection.commit()
            print("COMMIT")
            self.count_of_insert = 0
        if self.cnt == 500:  # беру 500 ответов и закрываю сокет
            self.close_connection_to_db()
            self.twm.stop()


def main():
    symbol = "BNBBTC"
    if len(sys.argv) > 1:
        symbol = sys.argv[1]
    print("symbol : ", symbol)

    twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)
    twm.start()  # обязательно до старта потоков прослушивания

    depth_stream_name = twm.start_depth_socket(
        callback=SocketStorage("bnbbtc@depth", twm, "binance").handle_socket_message,
        symbol=symbol,
    )

    kline_stream_name = twm.start_kline_socket(
        callback=SocketStorage("bnbbtc@kline", twm, "binance").handle_socket_message,
        symbol=symbol,
    )
    twm.join()
    print("End")
    print(depth_stream_name)
    print(kline_stream_name)


if __name__ == "__main__":
    main()
