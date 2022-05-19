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

def create_logger(name, exchange=None, symbol=None, folder="logs/", default_api=False):
    if not os.path.exists("logs/"):
        os.makedirs("logs/")

    logger = log.getLogger(name)
    logger.propagate = False
    filename = "logs/ListenerManager.log"
    if default_api:
        filename = "logs/API.log"
    if exchange != None:
        filename = f"{folder}{exchange}_{format_table_name(symbol)}.log"
    handler = log.FileHandler(filename)
    handler.setFormatter(log.Formatter("[" + name + " %(levelname)s]: {%(asctime)s} %(message)s"))

    logger.addHandler(handler)

    return logger
