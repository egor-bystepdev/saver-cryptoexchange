# saver-cryptoexchange
Service that provides API for listening and collecting data for different instruments from Binance and FTX cryptoexchanges.

## Installation guide
  ### Preinstalled requirements:
  - If you use manual install, you need Python 3.8 and corresponding pip3 (maybe newer versions are also suitable, but this is not guaranteed).
  - If you prefer docker-compose usage instead, you need installed and launched docker and docker-compose.
  - MySQL database installed and started. [Guide for Ubuntu](https://phoenixnap.com/kb/install-mysql-ubuntu-20-04).
  - Also you should have Binance and FTX API keys. [FTX](https://goodcrypto.app/how-to-configure-ftx-api-keys-and-add-them-to-good-crypto/) and [Binance](https://coinmatics.zendesk.com/hc/en-us/articles/360015574417-How-to-create-an-API-key-on-Binance) guides.
  ### Manual installation and run
  Once you have installed everything and downloaded all the necessary tools, you can move on to start up:
  - Clone project using `git clone https://github.com/egor-bystepdev/saver-cryptoexchange` and `cd saver-cryptoexchange`.
  - To install dependencies run `pip3 install -r requirements.txt`.
  - After that, put your API keys and secrets into environment variables `ftx_api_key`, `ftx_api_secret`, `binance_api_key` and `binance_api_secret`.
  - Also if and only if you have password for MySQL database (to check execute `sudo mysql -u root`, if no password field appear, skip that step) set ENV variable `sql_password` equals to it.
  - Optionally, adjust `listener/config.json` (it is responsible for instruments which start being listened immediately after API launch). For example, config.json can look like this:
    ```json
    {
      "checker_cooldown_s" : 600,
      "fatal_time_for_socket_s" : 300000,
      "symbols_in_start" : 
      [
          {
              "exchange" : "binance",
              "symbol"   : "BNBBTC"
          },
          {
              "exchange" : "ftx",
              "symbol"   : "BTC/USDT"
          }
      ]
    }
    ```

  - And finally, `python3 api.py` to run the API.
  ### Using with docker-compose
  - Put your API keys and secrets into environment variables `ftx_api_key`, `ftx_api_secret`, `binance_api_key` and `binance_api_secret`.
  - Also if and only if you have password for MySQL database (to check execute `sudo mysql -u root`, if no password field appear, skip that step) set ENV variable `sql_password` equals to it.
  - Run `docker-compose up -d`.

## Usage
  ### Get data and start/stop listening instruments
  - To get data, send GET request to `http://hostname:8080/` with set `exchange`, `instrument`, `start_timestamp` and `finish_timestamp` (in ms) parameters. For example, to get data for BNBBTC instrument from Binance exchange check URL `http://hostname:8080/?exchange=binance&instrument=BNBBTC&start_timestamp=1652991738435&finish_timestamp=1652991739435`.
  - To start listening for instrument, send GET request to `http://hostname:8080/start` with set parameters `exchange` and `instrument`. For instance, to start listening data for BTC/USDT pair from FTX exchange check URL<br/> `http://localhost:8080/start?exchange=ftx&instrument=BTC%2FUSDT`.
  - To stop listening of instrument, send similar GET request to `http://hostname:8080/stop`.
  ### Get Prometheus statistics
  To get metrics collected by Prometheus, check `http:://hostname:9090/graph`.
  ### Logs
  In `logs/` directory you can observe .log files, info and error messages about everything that happens in the programme are written there.
