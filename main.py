import listener
import json
import uvicorn
from fastapi import FastAPI
from fastapi import HTTPException

exchange_data_types = {
    "binance": ["trade", "kline", "depthUpdate"],
    "ftx": ["trades", "orderbook"]
}

exchanges = {"binance", "ftx"}

CRYPTO_API = FastAPI()


@CRYPTO_API.get("/")
def get_events(exchange: str, instrument: str, start_timestamp: int, finish_timestamp: int):
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

    return json.dumps(res)


@CRYPTO_API.get("/stop")
def stop(exchange: str, instrument: str):
    pass


@CRYPTO_API.get("/start")
def start(exchange: str, instrument: str):
    if exchange not in exchanges:
        log_text = f"not available exchange {exchange}"
        listener.handle_error("get_events api method", log_text, listener_db.logger)
        raise HTTPException(status_code=404, detail=log_text)
    started, log_text = listener_db.start_listing(exchange=exchange, symbol=instrument)
    if not started:
        listener.handle_error("start api method", log_text, listener_db.logger)
        raise HTTPException(status_code=404, detail=log_text)


if __name__ == "__main__":
    listener_db = listener.ListenerManager()
    uvicorn.run(CRYPTO_API, host="0.0.0.0", port=8000)
