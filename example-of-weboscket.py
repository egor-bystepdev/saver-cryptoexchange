import time
import calendar
import sqlite3
import json
import os

from binance import ThreadedWebsocketManager

api_key = os.environ["binance_api_key"]
api_secret = os.environ["binance_api_secret"]


# source: https://python-binance.readthedocs.io/en/latest/websockets.html

# прослушивание с биржи n ответов
# я пока написал всё на глобальной памяти, надо бы разобраться как прокидывать в хендлер переменные по ссылке


def main():
    def connect_to_db():
        global cursor
        global sqlite_connection
        try:
            sqlite_connection = sqlite3.connect("test.db")
            cursor = sqlite_connection.cursor()
            print("База данных создана и успешно подключена к SQLite")

            sqlite_select_query = "select sqlite_version();"
            cursor.execute(sqlite_select_query)
            record = cursor.fetchall()
            print("Версия базы данных SQLite: ", record)
            cursor.execute(
                """ SELECT count(name) FROM sqlite_master WHERE type='table' AND name='depth' """
            )
            if cursor.fetchone()[0] != 1:  # чек что такой таблицы ещё нет
                cursor.execute(
                    """CREATE TABLE depth
                        (time int, msg text)
                    """
                )
        except sqlite3.Error as error:
            print("Ошибка при подключении к sqlite", error)
            exit(0)

    symbol = "BNBBTC"

    global twm
    twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)
    twm.start()  # обязательно до старта потоков прослушивания
    global cnt
    cnt = 0

    def handle_socket_message(msg):
        global cnt
        if cnt == 0:
            connect_to_db()
        print(cnt)
        cnt += 1
        message = json.dumps(msg)  # перевожу dict в строку чтобы записать в бд
        cursor.executemany(
            "INSERT INTO depth (time, msg) VALUES (?,?)",
            [(int(calendar.timegm(time.gmtime())), message)],
        )
        if cnt == 15:  # беру 15 ответов и закрываю сокет
            twm.stop()
            sqlite_connection.commit()
            cursor.execute("""SELECT count(*) from depth""")
            print("count rows : ", cursor.fetchone())
            # если захочется посмотреть данные, то
            # curcor.execute("""select * from depth""")
            # print(cursor.fetchall())
            cursor.close()

    depth_stream_name = twm.start_depth_socket(
        callback=handle_socket_message, symbol=symbol
    )
    print("End")


if __name__ == "__main__":
    main()
