import os
import time
import dateutil.parser
import logging as log

def get_timestamp_ms_gtm0():
    return time.time_ns() // 1_000_000


def isoformattotimestamp(server_time: str):
    return int(dateutil.parser.isoparse(server_time).timestamp() * 1000)

def handle_error(function_name, err, logger):
    logger.error(f"Error in {function_name}")
    logger.error(err)

def create_logger(name):
    logger = log.getLogger(name)
    logger.propagate = False

    handler = log.StreamHandler()
    handler.setFormatter(log.Formatter("[" + name + " %(levelname)s]: %(message)s"))

    logger.addHandler(handler)

    return logger
