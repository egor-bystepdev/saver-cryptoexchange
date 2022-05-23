import logging as log
import time
import os

import dateutil.parser


def get_timestamp_ms_gtm0():
    return time.time_ns() // 1_000_000


def isoformattotimestamp(server_time: str):
    return int(dateutil.parser.isoparse(server_time).timestamp() * 1000)


def handle_error(function_name, err, logger):
    logger.error(f"Error in {function_name}")
    logger.error(err)

def format_table_name(name):
    return name.replace('/', '_').replace('-', '_')

def create_logger(name, exchange=None, symbol=None, folder="logs", default_api=False):
    if not os.path.exists("logs/"):
        os.makedirs("logs/")

    logger = log.getLogger(name)
    logger.propagate = False
    filename = "logs/ListenerManager.log"
    if default_api:
        filename = "logs/API.log"
    if exchange != None:
        dirname = f"{folder}/{exchange}/{format_table_name(symbol)}"
        if not os.path.exists(f"{folder}/{exchange}"):
            print("created 1")
            os.makedirs(f"{folder}/{exchange}")
        if not os.path.exists(dirname):
            print("created 2")
            os.makedirs(dirname)
        print('dirname', dirname)
        print("created 3")
        filename = f"{dirname}/{name.split()[0]}.log"
        print('filename', filename)
    handler = log.FileHandler(filename)
    handler.setFormatter(log.Formatter("[" + name + " %(levelname)s]: {%(asctime)s} %(message)s"))

    logger.addHandler(handler)

    return logger

def check_environment():
    def check_variable(name, fatal=True):
        if name not in os.environ:
            if fatal:
                print(f"!!! ENV variable {name} must be set !!!")
                exit(1)
            else:
                print(f"!!! Ensure that you don't have password on MySQL, otherwise set ENV variable {name} !!!")
    
    check_variable("binance_api_key")
    check_variable("binance_api_secret")
    check_variable("ftx_api_key")
    check_variable("ftx_api_secret")
    check_variable("sql_password", False)