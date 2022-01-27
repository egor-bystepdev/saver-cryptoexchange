from client import FtxWebsocketClient
import time

# client = FtxWebsocketClient()

# print(client.get_trades('BTC-PERP'))

print("==================================")

#time.sleep(3)

# f = client.get_trades('BTC-PERP')
#print(client.get_trades('BTC-PERP'))

import dateutil.parser

g = "2022-01-27T18:27:44.406233+00:00"

print(dateutil.parser.isoparse(g).timestamp())