import time
import requests
import logging
import datetime
import ssl
import json

from lib.utils import cur_time
from websocket import create_connection
from kafka import KafkaProducer

# 日志设置
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s (filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='huobi_kline.log',
                    filemode='a')


# kafka连接
def kafka_con():
    global producer
    producer = KafkaProducer(bootstrap_servers='47.75.116.175:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))


# websocket连接
def ws_connect():
    global ws
    ws = create_connection("wss://api.huobipro.com/ws", sslopt={"cert_reqs": ssl.CERT_NONE},
                           http_proxy_host="localhost", http_proxy_port=1080
                           )
    # 获取symbol
    symbol = requests.get('https://api.bitfinex.com/v1/symbols')
    symbols = json.loads(symbol.text)['data']

    symbols = [x for x in symbols.split(',')]
    logging.info("已获取到symbols")
    print('已获取symbols')
    # print(symbols)
    for sym in symbols:
        ws.send(json.dumps({"event": "subscribe", "channel": "candles", "key": 'trade:1D:' 't' + sym.upper()}))
        ws.send(json.dumps({"event": "subscribe", "channel": "candles", "key": 'trade:7D:' 't' + sym.upper()}))
        ws.send(json.dumps({"event": "subscribe", "channel": "candles", "key": 'trade:1M:' 't' + sym.upper()}))
        time.sleep(0.02)
