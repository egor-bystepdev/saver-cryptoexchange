FROM ubuntu:20.04
FROM python:latest
ARG sql_password
ENV sql_password=$sql_password
ARG binance_api_key
ENV binance_api_key=$binance_api_key
ARG binance_api_secret
ENV binance_api_secret=$binance_api_secret
ARG ftx_api_key
ENV ftx_api_key=$ftx_api_key
ARG ftx_api_secret
ENV ftx_api_secret=$ftx_api_secret
RUN mkdir -p /app
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
RUN pip3 install websocket-client
COPY . .
EXPOSE 8080
EXPOSE 9090
CMD [ "python3", "api.py" ]
