import listener
import json

from fastapi import FastAPI

app = FastAPI()


@app.get("/")
def get_events(exchange: str, instrument: str, start_timestamp: int, finish_timestamp: int):
    ans = listener.get_all_msg_in_db(exchange, instrument, start_timestamp,
                                     finish_timestamp, True)
    res = []
    for i in ans:
        tmp = '[' + i[1] + ']'
        res += json.loads(tmp)
    return json.dumps(res)
