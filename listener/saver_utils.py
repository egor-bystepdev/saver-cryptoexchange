import logging
import os
import traceback

from mysql.connector import connect, Error, errorcode

sql_password = os.environ["sql_password"]


def is_table_exists(cursor, name):
    cursor.execute("show tables like '" + name + "';")
    result_query = cursor.fetchall()
    return len(result_query) != 0


# возврат всех ответов по типу данных для биржи по интрументы с timestamp1 до timestamp2, возвращемое значение лист
# картежей, возможно надо будет ещё и чекать если ошибка в получении произошла
def get_all_msg_in_db(
        exchange: str,
        symbol: str,
        timestamp1: int,
        timestamp2: int,
        timestamp_in_ms: bool = False,
):
    if timestamp1 > timestamp2:
        return []

    try:
        if not timestamp_in_ms:
            timestamp1 *= 1000
            timestamp2 *= 1000
        bucket_size = 3 * 60 * 60 * 1000  # мб потом прокинется в переменные окружения
        start_timestamp = timestamp1 - timestamp1 % bucket_size
        finish_timestamp = timestamp2 + bucket_size - timestamp2 % bucket_size
        db_connection = connect(user="root", password=sql_password, host="127.0.0.1")
        cursor = db_connection.cursor()
        cursor.execute("use " + "_".join([exchange, symbol]) + ";")
        result = []
        for type_of_data in ["trade", "kline", "depthUpdate"]:
            for timestamp in range(start_timestamp, finish_timestamp, bucket_size):
                table_name = "_".join([type_of_data, str(timestamp)])

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
        cursor.close()
        return result
    except Error as err:
        print("Ошибка в get_all_msg_in_db:\n\t")
        if err.errno == errorcode.ProgrammingError:
            print("Синтаксическая ошибка в SQL запросе: ", err)
        elif err.errno == errorcode.IntegrityError:
            print("Проблема с записью ключей: ", err)
        elif err.errno == errorcode.DatabaseError:
            print("Ошибка с базой данных: ", err)
        elif err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Неправильный пароль или пользователь: ", err)
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Базы данных не существует: ", err)
        else:
            print(err)

        logging.error(traceback.format_exc())

        return []
