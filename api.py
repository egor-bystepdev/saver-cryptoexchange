import json
import time

import uvicorn
from fastapi import FastAPI, status
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from prometheus_client import start_http_server, Counter, Histogram
from prometheus_fastapi_instrumentator import Instrumentator

import listener

graphs = {}
graphs["counter"] = Counter(
    "app_http_request_count",
    "The Total number of HTTP Application request"
)
graphs["histogram"] = Histogram(
    "app_http_response_time",
    "The time of HTTP Application response",
    buckets=(1, 2, 5, 6, 10, float("inf"))  # Positive Infinity
)

exchange_data_types = {
    "binance": ["trade", "kline", "depthUpdate"],
    "ftx": ["trades", "orderbook"]
}

exchanges = {"binance", "ftx"}
CRYPTO_API = FastAPI()
instrumentator = Instrumentator(
    should_ignore_untemplated=True,
    should_instrument_requests_inprogress=True,
    excluded_handlers=["/metrics"],
    inprogress_name="inprogress",
    inprogress_labels=True,
)
instrumentator.instrument(CRYPTO_API).expose(CRYPTO_API)


@CRYPTO_API.get("/")
def get_events(exchange: str, instrument: str, start_timestamp: int, finish_timestamp: int):
    graphs["counter"].inc()
    start_time = time.time()
    if exchange not in exchanges:
        log_text = f"not available exchange {exchange}"
        listener.handle_error("get_events api method", log_text, listener_db.logger)
        raise HTTPException(status_code=404, detail=log_text)
    log_text, events = listener_db.get_all_messages(exchange, instrument, start_timestamp,
                                                    finish_timestamp, True, exchange_data_types[exchange])
    if log_text != "":
        listener.handle_error("get_events api method", log_text, listener_db.logger)
        raise HTTPException(status_code=404, detail=log_text)
    res = []
    for event in events:
        tmp = '[' + event[1] + ']'
        res += json.loads(tmp)
    end_time = time.time()
    graphs["histogram"].observe(end_time - start_time)
    print(graphs)
    return json.dumps(res)


@CRYPTO_API.get("/stop")
def stop(exchange: str, instrument: str):
    pass


@CRYPTO_API.get("/start")
def start(exchange: str, instrument: str):
    graphs["counter"].inc()
    start_time = time.time()
    if exchange not in exchanges:
        log_text = f"not available exchange {exchange}"
        listener.handle_error("get_events api method", log_text, listener_db.logger)
        raise HTTPException(status_code=404, detail=log_text)
    started, log_text = listener_db.start_listing(exchange=exchange, symbol=instrument)
    if not started:
        listener.handle_error("start api method", log_text, listener_db.logger)
        raise HTTPException(status_code=404, detail=log_text)
    end_time = time.time()
    graphs["histogram"].observe(end_time - start_time)
    print(graphs)
    return JSONResponse(status_code=status.HTTP_200_OK, content="DB started")


@CRYPTO_API.on_event("startup")
def startup_event():
    start_http_server(port=9090)
    listener_db = listener.ListenerManager()


if __name__ == "__main__":
    uvicorn.run(CRYPTO_API, host="0.0.0.0", port=8080)
