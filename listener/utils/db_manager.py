import os
import sys
import threading
import logging as log
from mysql.connector import connect, Error
from utils.helpers import handle_error, create_logger

class DBManager:
    def __init__(self, exchange, symbol, data_types, number=1):
        self.name= None
        self.cursor = None
        self.connection = None

        self.symbol = symbol
        self.exchange = exchange
        self.data_types = data_types

        self.lock = threading.Lock()
        self.password = os.environ["sql_password"]
        
        self.logger = create_logger(f"DBManager ({number})")
    
    def update_name(self):
        self.name = f"{self.exchange}_{self.symbol}"

    def connect(self):
        self.cursor = None
        self.connection = None
        try:
            self.update_name()

            self.logger.info(f"{self.symbol} connecting to {self.name}...\n")
            self.connection = connect(
                user="root", password=self.password, host="127.0.0.1"
            )

            self.cursor = self.connection.cursor()
            self.cursor.execute("CREATE DATABASE IF NOT EXISTS " + self.name)
            self.cursor.execute("USE " + self.name)
            self.logger.info("Database created and succesfully connected to MySQL")

            self.cursor.execute("SELECT VERSION()")
            record = self.cursor.fetchall()
            self.logger.info(f"Database version: {record[0][0]}\n")
        except Error as err:
            handle_error("connect_to_db", err, self.logger)
            sys.exit(1) # if connection is not successfull, thread should be relaunched
    
    def disconnect(self):
        try:
            self.connection.commit()
            self.cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")

            self.logger.info(f"count rows : {self.cursor.fetchone()}")
            self.cursor.close()
        except Error as err:
            handle_error("close_connection_to_db", err, self.logger)
    
    def create_tables(self, current_time_for_table_name):
        for data_type in self.data_types:
            try:
                self.cursor.execute(
                    "CREATE TABLE IF NOT EXISTS "
                    + "_".join([data_type, str(current_time_for_table_name)])
                    + "(timestamp BIGINT, data TEXT);"
                )
            except Error as err:
                handle_error("create_table", err, self.logger)
                sys.exit(1) # if any table could not be created, thread should be relaunched
    
    def insert(self, table_name, server_time, message, cnt):
        with self.lock:
            try:
                self.cursor.execute(
                    "INSERT INTO "
                    + table_name
                    + " (timestamp, data) VALUES ("
                    + str(server_time)
                    + ', "'
                    + message
                    + '");'
                )
            except Error as err:
                handle_error("insert", err, log)
            
            self.connection.commit()
            self.logger.info(f"{cnt} COMMIT\n")

    def get_all_messages(self, time_bucket_db, timestamp1, timestamp2, timestamp_in_ms=False, data_types = []):
        if timestamp1 > timestamp2:
            return []

        try:
            if not timestamp_in_ms:
                timestamp1 *= 1000
                timestamp2 *= 1000
            start_timestamp = timestamp1 - timestamp1 % time_bucket_db
            finish_timestamp = timestamp2 + (time_bucket_db - timestamp2 % time_bucket_db)

            result = []
            for data_type in data_types:
                for timestamp in range(start_timestamp, finish_timestamp, time_bucket_db):
                    table_name = "_".join([data_type, str(timestamp)])

                    with self.lock:
                        query = f"SELECT * FROM {table_name} WHERE timestamp >= {timestamp1} AND timestamp <= {timestamp2};"
                        self.logger.info(f"QUERY: {query}")

                        self.cursor.execute(query)
                        result += self.cursor.fetchall()

            return result
        except Error as err:
            handle_error("get_all_messages", err, self.logger)

            return []


