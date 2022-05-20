from curses.ascii import HT
import requests
from http import HTTPStatus
import time
import json


def test_check_status_code_equals_200():
    response = requests.get("http://127.0.0.1:8080/",
                            params={'exchange': "binance", 'instrument': "BNBBTC", 'start_timestamp': 0,
                                    'finish_timestamp': 0})
    assert response.status_code == HTTPStatus.OK


def test_check_status_code_equals_422():
    response = requests.get("http://127.0.0.1:8080/",
                            params={'exchange': "binance", 'instrument': "BNBBTC", 'start_timestamp': 1642604400000})
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY

def test_all_methods():
    response = requests.get("http://127.0.0.1:8080/start",
                            params={'exchange': "ftx", 'instrument': "BTC/USDT"})
    assert response.status_code == HTTPStatus.OK

    time.sleep(5)

    current_time = time.time_ns() // 1_000_000
    response = requests.get("http://127.0.0.1:8080/",
                            params={
                                'exchange': "ftx",
                                'instrument': "BTC/USDT",
                                'start_timestamp': current_time - 10000,
                                "finish_timestamp": current_time + 10000})
    assert response.status_code == HTTPStatus.OK

    json_resp = json.loads(response.json())
    assert len(json_resp) > 0

    response = requests.get("http://127.0.0.1:8080/stop",
                            params={'exchange': "ftx", 'instrument': "BTC/USDT"})
    assert response.status_code == HTTPStatus.OK

def test_double_start():
    response = requests.get("http://127.0.0.1:8080/start",
                            params={'exchange': "ftx", 'instrument': "BTC/USDT"})
    assert response.status_code == HTTPStatus.OK

    response = requests.get("http://127.0.0.1:8080/start",
                            params={'exchange': "ftx", 'instrument': "BTC/USDT"})
    assert response.status_code == HTTPStatus.CONFLICT

    response = requests.get("http://127.0.0.1:8080/stop",
                            params={'exchange': "ftx", 'instrument': "BTC/USDT"})
    assert response.status_code == HTTPStatus.OK