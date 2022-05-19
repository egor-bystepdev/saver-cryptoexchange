import json
import time

import uvicorn
from fastapi import FastAPI, status
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from prometheus_client import start_http_server, Counter, Gauge
from prometheus_fastapi_instrumentator import Instrumentator
from listener.listener_manager import ListenerManager
from listener.utils.helpers import handle_error, create_logger

api_logger = create_logger("API", default_api=True)
graphs = {}
graphs["counter_errors"] = Counter(
    "app_http_request_count_errors", "The Total number of HTTP Application request with errors"
)
graphs["counter_start"] = Counter(
    "app_http_request_count_start", "The Total number of HTTP Application request with start (number of instruments)"
)
graphs["gauge"] = Gauge('response_api_time', 'api time response')

exchange_data_types = {
    "binance": ["trade", "kline", "depthUpdate"],
    "ftx": ["trades", "orderbook"],
}

exchanges = {"binance", "ftx"}
CRYPTO_API = FastAPI()
listener_db = ListenerManager()

instrumentator = Instrumentator(
    should_ignore_untemplated=True,
    should_instrument_requests_inprogress=True,
    excluded_handlers=["/metrics"],
    inprogress_name="inprogress",
    inprogress_labels=True,
)
instrumentator.instrument(CRYPTO_API).expose(CRYPTO_API)


@CRYPTO_API.get("/")
def get_events(
        exchange: str, instrument: str, start_timestamp: int, finish_timestamp: int
):
    api_logger.info(
        f"get_event with exchange {exchange} with intrument {instrument} with start_timestamp"
        f" {start_timestamp} with finish_timestamp {start_timestamp}")
    start_time = time.time()
    if exchange not in exchanges:
        log_text = f"not available exchange {exchange}"
        handle_error("get_events api method", log_text, api_logger)
        graphs["counter_errors"].inc()
        raise HTTPException(status_code=404, detail=log_text)
    log_text, events = listener_db.get_all_messages(
        exchange,
        instrument,
        start_timestamp,
        finish_timestamp,
        True,
        exchange_data_types[exchange],
    )
    if log_text != "":
        handle_error("get_events api method", log_text, api_logger)
        graphs["counter_errors"].inc()
        raise HTTPException(status_code=409, detail=log_text)
    res = []
    for event in events:
        tmp = "[" + event[1] + "]"
        res += json.loads(tmp.replace('\\', ''))
    end_time = time.time()
    graphs["gauge"].set(end_time - start_time)
    return json.dumps(res)


@CRYPTO_API.get("/stop")
def stop(exchange: str, instrument: str):
    api_logger.info(
        f"stop with exchange {exchange} with intrument {instrument}")
    if exchange not in exchanges:
        log_text = f"not available exchange {exchange}"
        handle_error("get_events api method", log_text, api_logger)
        graphs["counter_errors"].inc()
        raise HTTPException(status_code=404, detail=log_text)
    started, log_text = listener_db.stop_listening(exchange=exchange, symbol=instrument)
    if not started:
        graphs["counter_errors"].inc()
        handle_error("start api method", log_text, api_logger)
        raise HTTPException(status_code=409, detail=log_text)
    return JSONResponse(status_code=status.HTTP_200_OK, content=log_text)


@CRYPTO_API.get("/start")
def start(exchange: str, instrument: str):
    api_logger.info(
        f"start with exchange {exchange} with intrument {instrument}")
    if exchange not in exchanges:
        log_text = f"not available exchange {exchange}"
        handle_error("get_events api method", log_text, api_logger)
        graphs["counter_errors"].inc()
        raise HTTPException(status_code=404, detail=log_text)
    started, log_text = listener_db.start_listing(exchange=exchange, symbol=instrument)
    if not started:
        graphs["counter_errors"].inc()
        handle_error("start api method", log_text, api_logger)
        raise HTTPException(status_code=409, detail=log_text)
    graphs["counter_start"].inc()
    return JSONResponse(status_code=status.HTTP_200_OK, content=log_text)


@CRYPTO_API.on_event("startup")
def startup_event():
    api_logger.info("API started")
    start_http_server(port=9090)


if __name__ == "__main__":
    uvicorn.run(CRYPTO_API, host="0.0.0.0", port=8080)
