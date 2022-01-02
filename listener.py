import time
import json
import os
import sys
from mysql.connector import connect, Error

from binance import ThreadedWebsocketManager

api_key = os.environ["binance_api_key"]
api_secret = os.environ["binance_api_secret"]
sql_password = os.environ["sql_password"]


# source: https://python-binance.readthedocs.io/en/latest/websockets.html
# прослушивание с биржи n ответов


def get_timestamp_ms_gtm0():
    return time.time_ns() // 1_000_000


class SocketStorage:
    def __init__(self, name, twm, exchange, symbol):
        self.symbol = symbol
        self.stream_name = name
        self.cnt = 0
        self.twm = twm
        self.exchange = exchange
        self.capacity_for_db = 1000  # после скольких инсертов делаем коммит
        self.count_of_insert = 0
        self.time_bucket_db = 3 * 60 * 60 * 1000  # -- как часто обновляем бд, всё в ms
        self.table_name = None
        self.current_time_for_table_name = None

    def upd_db_name(self):
        self.db_name = self.exchange + "_" + self.symbol + "_" + self.type_of_data

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
        self.db_connection = None
        try:
            self.upd_db_name()
            print(self.type_of_data, " connect", self.db_name)
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
        self.cursor.execute(
            "CREATE TABLE IF NOT EXISTS "
            + self.table_name
            + "(timestamp BIGINT PRIMARY KEY, data TEXT);"
        )

    def close_connection_to_db(self):
        self.db_connection.commit()
        self.cursor.execute("""SELECT count(*) from """ + self.table_name)
        print("count rows : ", self.cursor.fetchone())
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
        #if self.cnt == 5000000:  # беру 500 ответов и закрываю сокет
        #    self.close_connection_to_db()
        #    self.twm.stop()


def main():
    symbol = "BNBBTC"
    if len(sys.argv) > 1:
        symbol = sys.argv[1]
    print("symbol : ", symbol)

    twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)
    twm.start()  # обязательно до старта потоков прослушивания

    depth_stream_name = twm.start_depth_socket(
        callback=SocketStorage(
            "bnbbtc@depth", twm, "binance", symbol
        ).handle_socket_message,
        symbol=symbol,
    )

    kline_stream_name = twm.start_kline_socket(
        callback=SocketStorage(
            "bnbbtc@kline", twm, "binance", symbol
        ).handle_socket_message,
        symbol=symbol,
    )
    twm.join()
    print("End")
    print(depth_stream_name)
    print(kline_stream_name)


if __name__ == "__main__":
    main()
