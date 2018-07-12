import ssl
import websocket
import datetime
import json
import time
import requests
import logging
import threading

from websocket import create_connection
from kafka import KafkaProducer
from flask import Flask, jsonify, make_response

app = Flask(__name__)
# 日志设置
# logging.basicConfig(level=logging.DEBUG,
#                     format='%(asctime)s (filename)s[line:%(lineno)d] %(levelname)s %(message)s',
#                     datefmt='%a, %d %b %Y %H:%M:%S',
#                     filename='bitfinex_depth.log',
#                     filemode='a')


# 本地时间转换为13位
def cur_time():
    t1 = datetime.datetime.now()
    t2 = t1.strftime("%Y-%m-%d %H:%M:%S")
    return t2


# kafka连接
def kafka_con():
    global producer
    producer = KafkaProducer(bootstrap_servers='47.75.116.175:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))


# websocket连接
def ws_connect():
    global ws
    ws = create_connection("wss://api.bitfinex.com/ws/2", sslopt={"cert_reqs": ssl.CERT_NONE},
                           # http_proxy_host="localhost", http_proxy_port=1080
                           )
    # 获取symbol
    symbol = requests.get('https://api.bitfinex.com/v1/symbols')
    symbols = symbol.text.replace('[', '').replace(']', '').replace('"', '')
    symbols = [x for x in symbols.split(',')]
    logging.info("已获取到symbols")
    print('已获取symbols')
    # print(symbols)
    for sym in symbols:
        ws.send(json.dumps({"event": "subscribe", "channel": "book", "symbol": 't' + sym.upper()}))


# 定义一个字典做映射
maps = {}


def get_detail(flag):
    while True:
        if flag == 1:
            print('循环中止')
            break
        detail_ls = json.loads(ws.recv())
        if isinstance(detail_ls, dict):
            if detail_ls['event'] == 'subscribed':
                maps[detail_ls['chanId']] = detail_ls['symbol'][1:].upper()
                # print(maps)
                logging.info("maps插入一条新的映射")
        if isinstance(detail_ls, list):
            if detail_ls[1] == "hb" or detail_ls[1] == []:
                continue
            # 如果返回的是第一个websocket大列表并且对应的socket在映射中
            elif detail_ls[0] in maps.keys() and len(detail_ls[1]) > 3:
                pair = maps[detail_ls[0]]
                dic = {
                    "onlyKey": "Bitfinex_" + pair[:3] + '_' + pair[-3:],
                    "measurement": "Depth",
                    "timestamp": cur_time(),
                    "tick": {
                        'bids': [],
                        'asks': [],
                    },
                    "type": 0,
                }
                for _ in detail_ls[1]:
                    tem = {
                        "price": _[0],
                        "count": _[1],
                        "amount": _[2]
                    }
                    if _[2] < 0:
                        dic['tick']['bids'].append(tem)
                    else:
                        dic['tick']['asks'].append(tem)
                producer.send('depth-dev', [dic])
                logging.info("send first %s successful > timestamp--%s" % (pair, cur_time()))
                # print(("send first %s successful > timestamp--%s" % (pair, cur_time())))

            # 判断该id是否在映射记录里面
            elif detail_ls[0] in maps.keys() and len(detail_ls[1]) == 3:
                pair = maps[detail_ls[0]]
                change_type = "bids" if detail_ls[1][2] < 0 else 'asks'
                dic = {
                    "onlyKey": "Bitfinex_" + pair[:3] + '_' + pair[-3:],
                    "measurement": "Depth",
                    "timestamp": cur_time(),
                    "tick": {
                        change_type: [
                            {
                                'price': detail_ls[1][0],
                                'count': detail_ls[1][1],
                                'amount': detail_ls[1][2],
                            }
                        ]
                    },
                    "type": 1,
                }
                producer.send('depth-dev', [dic])
                logging.info("send successful > timestamp--%s" % cur_time())
                # print('send successful')
        else:
            logging.info("类型有误")
            continue


# 端口提供ws重连
@app.route('/job/restart', methods=['GET', 'POST'])
def add_task():
    ws.close()
    print('ws已关闭')
    get_detail(1)
    print('循环已关闭')
    print('正在重连')
    try:
        ws_connect()
    except Exception as e:
        logging.info('连接异常, 等待5秒后重连')
        print('连接异常, 等待5秒后重连')
        time.sleep(5)
        ws_connect()
    kafka_con()
    logging.info("kafka已连接")
    print('kafka已连接')
    print('开始获取数据')
    get_detail(0)


# 端口提供ws连接
@app.route('/job/start', methods=['GET', 'POST'])
def open_task():
    try:
        ws_connect()
        print('ws已连接')
    except Exception as e:
        logging.info('连接异常, 等待5秒后重连')
        print('连接异常, 等待5秒后重连')
        time.sleep(5)
        ws_connect()
    kafka_con()
    logging.info("kafka已连接")
    print('kafka已连接')
    print('开始获取数据')
    get_detail(0)


@app.route('/')
def index():
    return '<h1>OK</h1>'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
