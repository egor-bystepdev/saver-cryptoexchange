import threading
import time
from websocketsftx.client import FtxWebsocketClient
	
def create_thread_for_stream(symbol, data_type, callback):
	client = FtxWebsocketClient()
	while True:
		if data_type == "trades":
			callback(client.get_trades(symbol), data_type)
		elif data_type == "orderbook":
			callback(client.get_orderbook(symbol), data_type)
		time.sleep(0.5)

class ThreadedWebsocketManagerFTX:
	def __init__(self, data_types : list, sleep_time, symbol : str):
		self.data_types = data_types
		self.sleep_time = sleep_time
		self.symbol = symbol
	
	def start(self, callback):
		self.threads = []
		for data_type in self.data_types:
			self.threads.append(threading.Thread(target=create_thread_for_stream, args=(self.symbol, data_type, callback,)))

		for thread in self.threads:
			thread.start()
	
	def join(self):
		for thread in self.threads:
			thread.join()