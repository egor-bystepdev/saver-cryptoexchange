from curses.ascii import HT
import requests
from http import HTTPStatus


def test_check_status_code_equals_200():
    response = requests.get("http://127.0.0.1:8000/",
                            params={'exchange': "binance", 'instrument': "BNBBTC", 'start_timestamp': 0,
                                    'finish_timestamp': 0})
    assert response.status_code == HTTPStatus.OK

def test_check_status_code_equals_422():
    response = requests.get("http://127.0.0.1:8000/",
                            params={'exchange': "binance", 'instrument': "BNBBTC", 'start_timestamp': 1642604400000})

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
