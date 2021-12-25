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


class SocketStorage:
    def __init__(self, name, twm):
        self.stream_name = name
        self.cnt = 0
        self.twm = twm

    def connect_to_db(self):
        self.cursor = None
        self.sqlite_connection = None
        try:
            self.sqlite_connection = sqlite3.connect("test.db")
            self.cursor = self.sqlite_connection.cursor()
            print("База данных создана и успешно подключена к SQLite")

            self.sqlite_select_query = "select sqlite_version();"
            self.cursor.execute(self.sqlite_select_query)
            record = self.cursor.fetchall()
            print("Версия базы данных SQLite: ", record)
            self.cursor.execute(
                """ SELECT count(name) FROM sqlite_master WHERE type='table' AND name='depth' """
            )
            if self.cursor.fetchone()[0] != 1:  # чек что такой таблицы ещё нет
                self.cursor.execute(
                    """CREATE TABLE depth
                        (time int, msg text)
                    """
                )
            return True
        except sqlite3.Error as error:
            print("Ошибка при подключении к sqlite", error)
            return False

    def handle_socket_message(self, msg):
        if self.cnt == 0:
            if self.connect_to_db() == False:
                self.twm.stop()
        print(self.cnt)
        self.cnt += 1
        message = json.dumps(msg)  # перевожу dict в строку чтобы записать в бд
        self.cursor.executemany(
            "INSERT INTO depth (time, msg) VALUES (?,?)",
            [(int(calendar.timegm(time.gmtime())), message)],
        )
        if self.cnt == 5:  # беру 15 ответов и закрываю сокет
            self.sqlite_connection.commit()
            self.cursor.execute("""SELECT count(*) from depth""")
            print("count rows : ", self.cursor.fetchone())
            # если захочется посмотреть данные, то
            # curcor.execute("""select * from depth""")
            # print(cursor.fetchall())
            self.cursor.close()
            self.twm.stop()


def main():

    symbol = "BNBBTC"

    twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)
    twm.start()  # обязательно до старта потоков прослушивания

    depth_handler = SocketStorage(
        "bnbbtc@depth", twm
    )  # here we have problem with param <name>, bcs idk how find actual name, maybe symbol.to_lower() + "@" + type
    # i think we won't to stop socket, it'a opportunity only for testing

    depth_stream_name = twm.start_depth_socket(
        callback=depth_handler.handle_socket_message, symbol=symbol
    )
    twm.join()
    print("End")
    print(depth_stream_name)


if __name__ == "__main__":
    main()
