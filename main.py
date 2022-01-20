import listener
import json
import uvicorn

from fastapi import FastAPI

CRYPTO_API = FastAPI()


@CRYPTO_API.get("/")
def get_events(exchange: str, instrument: str, start_timestamp: int, finish_timestamp: int):
    events = listener.get_all_msg_in_db(exchange, instrument, start_timestamp,
                                     finish_timestamp, True)
    res = []
    for event in events:
        tmp = '[' + event[1] + ']'
        res += json.loads(tmp)
    return json.dumps(res)


if __name__ == "__main__":
    uvicorn.run(CRYPTO_API, host="0.0.0.0", port=8000)
