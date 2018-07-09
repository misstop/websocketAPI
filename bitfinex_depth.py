import ssl
import websocket
import datetime
import json
import time
import requests
import logging

from websocket import create_connection
from kafka import KafkaProducer

# 日志设置
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s (filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='bitfinex_depth.log',
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
        ws.send(json.dumps({"event": "subscribe", "channel": "book", "symbol": 't' + sym.upper()}))


ws_connect()
logging.info("ws已发送")
print('ws已连接')
kafka_con()
logging.info("kafka已连接")
print('kafka已连接')

# 定义一个字典做映射
maps = {}

while True:
    try:
        # try:
        detail_ls = json.loads(ws.recv())
        # except Exception as e:
        #     logging.info(e)
        #     continue
        # 判断返回的是否为字典
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
                    }
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
                print(("send first %s successful > timestamp--%s" % (pair, cur_time())))

            # 判断该id是否在映射记录里面
            elif detail_ls[0] in maps.keys() and len(detail_ls[1]) == 3:
                pair = maps[detail_ls[0]]
                change_type = "bid" if detail_ls[1][2] < 0 else 'ask'
                dic = {
                    "onlyKey": "Bitfinex_"+pair[:3]+'_'+pair[-3:],
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
                    }
                }
                logging.info(dic)
                # print(dic)
                producer.send('depth-dev', [dic])
                logging.info("send successful > timestamp--%s" % cur_time())
                print('send successful')
        else:
            logging.info("类型有误")
            continue
    except Exception as e:
        try:
            print(e)
            print("ws重连 时间--%s" % cur_time())
            time.sleep(1)
            maps = {}
            ws_connect()
        except Exception as e:
            print(e)
            print("重连失败，等五秒再次尝试 时间%s" % cur_time())
            time.sleep(5)
            maps = {}
            ws_connect()