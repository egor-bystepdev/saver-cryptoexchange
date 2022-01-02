from mysql.connector import connect, Error
import logging
import traceback
import os

sql_password = os.environ["sql_password"]


def is_table_exists(cursor, name):
    cursor.execute("show tables like '" + name + "';")
    result_query = cursor.fetchall()
    if len(result_query) != 0:
        return True
    else:
        return False


# возврат всех ответов по типу данных для биржи по интрументы с timestamp1 до timestamp, возвращемое значение лист картежей, возможно надо будет ещё и чекать если ошибка в получении произошла
def get_all_msg_in_db(
    exchange: str,
    symbol: str,
    data_type: str,
    timestamp1: int,
    timestamp2: int,
    timestamp_in_ms=False,
):
    try:
        if not timestamp_in_ms:
            timestamp1 *= 1000
            timestamp2 *= 1000
        bucket_size = 3 * 60 * 60 * 1000  # мб потом прокинется в переменные окружения
        timestamp1 -= timestamp1 % bucket_size
        timestamp2 += bucket_size - timestamp2 % bucket_size
        db_connection = connect(user="root", password=sql_password, host="127.0.0.1")
        cursor = db_connection.cursor()
        cursor.execute("use " + "_".join([exchange, symbol, data_type]) + ";")
        result = []
        for timestamp in range(timestamp1, timestamp2, bucket_size):
            table_name = "_".join(["data", str(timestamp)])

            if is_table_exists(cursor, table_name):
                cursor.execute(
                    "select * from "
                    + table_name
                    + " where timestamp >= "
                    + str(timestamp1)
                    + " and timestamp <= "
                    + str(timestamp2)
                    + ";"
                )
                result += cursor.fetchall()
            else:
                print("kekos")
        cursor.close()
        return result

    except Exception as e:
        logging.error(traceback.format_exc())
        return []


#  f = get_all_msg_in_db("binance", "BNBBTC", "depthUpdate", 1640808000000, 1641153600001, timestamp_in_ms=True)
