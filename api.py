from flask import Flask
from flask_restful import Api, Resource

import listener
import json

app = Flask(__name__)
api = Api(app)


class BinanceAPI(Resource):
    @staticmethod
    def get_events(exchange, instrument, start_timestamp, finish_timestamp):
        ans = listener.get_all_msg_in_db(exchange, instrument, start_timestamp, finish_timestamp) # хочу понять, надо ли как-то изменять ответный джейсон, изменять поля или разделять по data_type
        res = []
        for i in ans:
            tmp = '[' + i[3] + ']'
            res += json.loads(tmp)
        return json.dumps(res)




api.add_resource(BinanceAPI, "/", "/api")
if __name__ == '__main__':
    app.run(debug=True)
