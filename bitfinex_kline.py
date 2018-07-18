import ssl
import websocket
import json
import time
import requests
import logging
import datetime

from websocket import create_connection
from kafka import KafkaProducer

# 日志设置
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s (filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='logs/bitfinex_kline.log',
                    filemode='a')


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
                           http_proxy_host="localhost", http_proxy_port=1080
                           )
    # 获取symbol
    symbol = requests.get('https://api.bitfinex.com/v1/symbols')
    symbols = symbol.text.replace('[', '').replace(']', '').replace('"', '')
    symbols = [x for x in symbols.split(',')]
    logging.info("已获取到symbols")
    print('已获取symbols')
    # print(symbols)
    for sym in symbols:
        ws.send(json.dumps({"event": "subscribe", "channel": "candles", "key": 'trade:1D:' 't' + sym.upper()}))
        ws.send(json.dumps({"event": "subscribe", "channel": "candles", "key": 'trade:7D:' 't' + sym.upper()}))
        ws.send(json.dumps({"event": "subscribe", "channel": "candles", "key": 'trade:1M:' 't' + sym.upper()}))
        time.sleep(0.02)


ws_connect()
logging.info("ws已发送")
print('ws已连接')
kafka_con()
logging.info("kafka已连接")
print('kafka已连接')

# 定义一个字典做映射
maps = {}

i = 0

while True:
    try:
        detail_ls = json.loads(ws.recv())
        if isinstance(detail_ls, dict):
            if detail_ls['event'] == 'subscribed':
                maps[detail_ls['chanId']] = detail_ls['key']
                i += 1
                print(maps)
                logging.info("maps插入第%s条映射" % i)
                print("maps插入第%s条映射" % i)
        elif isinstance(detail_ls, list):
            if detail_ls[1] == "hb" or detail_ls[1] == []:
                continue
            # 如果返回的是第一个websocket大列表并且对应的socket在映射中
            elif detail_ls[0] in maps.keys() and isinstance(detail_ls[1][0], list):
                pair = maps[detail_ls[0]][6:]
                for _ in detail_ls[1]:
                    dic = {
                        "close": _[2],  # 昨收价
                        "exchange": "Bitfinex",  # 交易所
                        "high": _[3],  # 最高
                        "low": _[4],  # 最低
                        "measurement": "kline_"+pair[:2],  # 来源
                        "onlyKey": "Bitfinex_%s_%s" % (pair[4:7], pair[7:10]),  # 交易对
                        "open": _[1],  # 开盘价
                        "symbol": pair[4:7],  # 左交易对
                        "timestamp": _[0],  # 时间戳
                        "unit": pair[7:10],  # 右交易对
                        "volume": _[5]  # 数量
                    }
                    producer.send('kline-test', [dic])
                logging.info("send first %s successful > timestamp--%s" % (pair, cur_time()))
                print(("send first %s successful > timestamp--%s" % (pair, cur_time())))
            elif detail_ls[0] in maps.keys() and len(detail_ls[1]) == 6:
                pair = maps[detail_ls[0]][6:]
                dic = {
                    "close": detail_ls[1][2],  # 昨收价
                    "exchange": "Bitfinex",  # 交易所
                    "high": detail_ls[1][3],  # 最高
                    "low": detail_ls[1][4],  # 最低
                    "measurement": "kline_" + pair[:2],  # 来源
                    "onlyKey": "Bitfinex_%s_%s" % (pair[4:7], pair[7:10]),  # 交易对
                    "open": detail_ls[1][1],  # 开盘价
                    "symbol": pair[4:7],  # 左交易对
                    "timestamp": detail_ls[1][0],  # 时间戳
                    "unit": pair[7:10],  # 右交易对
                    "volume": detail_ls[1][5]  # 数量
                }
                # print(dic)
                producer.send('kline-test', [dic])
                logging.info("%s ---send successful > timestamp--%s" % (dic['onlyKey'], cur_time()))
                print("%s ---send successful > timestamp--%s" % (dic['onlyKey'], cur_time()))
        else:
            logging.info("返回的是无用的数据")
            continue
    except Exception as e:
        try:
            print(e)
            logging.info("ws重连 时间--%s" % cur_time())
            print("ws重连 时间--%s" % cur_time())
            time.sleep(1)
            maps = {}
            i = 0
            ws_connect()
        except Exception as e:
            print(e)
            print("重连失败，等五秒再次尝试 时间%s" % cur_time())
            time.sleep(5)
            maps = {}
            i = 0
            ws_connect()