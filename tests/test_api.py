import requests


def test_check_status_code_equals_200():
    response = requests.get("http://127.0.0.1:8000/",
                            params={'exchange': "binance", 'instrument': "BNBBTC", 'start_timestamp': 1642604400000,
                                    'finish_timestamp': 1642615078957})
    assert response.status_code == 200
