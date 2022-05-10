import listener
import json
import uvicorn

exchange_data_types = {
	"binance": ["trade", "kline", "depthUpdate"],
	"ftx": ["trades", "orderbook"]
}

from fastapi import FastAPI
from fastapi import HTTPException

CRYPTO_API = FastAPI()

@CRYPTO_API.get("/")
def get_events(exchange: str, instrument: str, start_timestamp: int, finish_timestamp: int):
    events = listener.get_all_msg_in_db(exchange, instrument, start_timestamp,
                                     finish_timestamp, True, exchange_data_types[exchange])
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
    started, log_text = listener_db.start_listing(exchange=exchange, symbol=instrument)
    if not started:
        listener.handle_error("start api method", log_text, listener_db.logger)
        raise HTTPException(status_code=404, detail=log_text)



if __name__ == "__main__":
    listener_db = listener.ListenerManager()
    uvicorn.run(CRYPTO_API, host="0.0.0.0", port=8000)
