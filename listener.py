import time
import calendar
import sqlite3
import json
import os
import threading

from binance import ThreadedWebsocketManager

api_key = os.environ["binance_api_key"]
api_secret = os.environ["binance_api_secret"]


# source: https://python-binance.readthedocs.io/en/latest/websockets.html
# прослушивание с биржи n ответов


def get_timestamp_ms_gtm0():
    return time.time_ns() // 1_000_000


class SocketStorage:
    def __init__(self, name, twm, exchange):
        self.stream_name = name
        self.cnt = 0
        self.twm = twm
        self.exchange = exchange
        self.capacity_for_db = 1000  # после скольких инсертов делаем коммит
        self.count_of_insert = 0
        self.time_bucket_db = 3 * 60 * 60 * 1000  # -- как часто обновляем бд, всё в ms

    def get_db_path(self):
        self.db_path = self.exchange + "_" + str(self.current_db_time)

    def connect_to_db(self):
        self.cursor = None
        self.sqlite_connection = None
        try:
            self.sqlite_connection = sqlite3.connect(self.db_path + ".db")
            self.cursor = self.sqlite_connection.cursor()
            print("База данных создана и успешно подключена к SQLite")

            self.sqlite_select_query = "select sqlite_version();"
            self.cursor.execute(self.sqlite_select_query)
            record = self.cursor.fetchall()
            print("Версия базы данных SQLite: ", record)
            self.cursor.execute(
                """ SELECT count(name) FROM sqlite_master WHERE type='table' AND name='"""
                + self.type_of_data
                + """' """
            )
            if self.cursor.fetchone()[0] != 1:  # чек что такой таблицы ещё нет
                self.cursor.execute(
                    """CREATE TABLE """
                    + self.type_of_data
                    + """
                         (time int, msg text)
                    """
                )
            return True
        except sqlite3.Error as error:
            print("Ошибка при подключении к sqlite", error)
            return False

    def close_connection_to_db(self):
        self.sqlite_connection.commit()
        self.cursor.execute("""SELECT count(*) from """ + self.type_of_data)
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
            self.current_db_time = server_time - server_time % self.time_bucket_db
            self.get_db_path()
            if self.connect_to_db() == False:
                self.twm.stop()  # !
        elif receive_time >= self.current_db_time + self.time_bucket_db:
            self.close_connection_to_db()
            self.current_db_time += self.time_bucket_db
            self.get_db_path()
            if self.connect_to_db() == False:
                self.twm.stop()  # !

        print(self.cnt)
        self.cnt += 1
        message = json.dumps(msg)  # перевожу dict в строку чтобы записать в бд
        self.count_of_insert += 1
        self.cursor.executemany(
            "INSERT INTO " + self.type_of_data + " (time, msg) VALUES (?,?)",
            [(receive_time, message)],
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

    twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)
    twm.start()  # обязательно до старта потоков прослушивания

    depth_stream_name = twm.start_depth_socket(
        callback=SocketStorage("bnbbtc@depth", twm, "binance").handle_socket_message,
        symbol=symbol,
    )
    twm.join()
    print("End")
    print(depth_stream_name)


if __name__ == "__main__":
    main()
