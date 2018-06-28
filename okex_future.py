import ssl, websocket, json, time
from websocket import create_connection
from kafka import KafkaProducer


# 本地时间转换为19位
def cur_time():
    t1 = time.time()
    t2 = int(t1 * 1000)
    t3 = t2 * 1000000
    return t3


# kafka连接
def kafka_con():
    global producer
    producer = KafkaProducer(bootstrap_servers='47.75.116.175:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))


# websocket连接
def ws_connect():
    global ws
    ws = create_connection("wss://real.okex.com:10440/websocket/okexapi", sslopt={"cert_reqs": ssl.CERT_NONE},
                           http_proxy_host="localhost", http_proxy_port=1080)
    ws.send(json.dumps({'event': 'addChannel', 'channel': 'ok_sub_futureusd_btc_trade_next_week'}))


ws_connect()
print("ws已连接")
kafka_con()
print("kafka已连接")


"""
[{"binary":0,"channel":"addChannel","data":{"result":true,"channel":"ok_sub_futureusd_btc_trade_next_week"}}]
[{
    "binary":0,
    "channel":"ok_sub_futureusd_btc_trade_next_week",
    "data":[
        ["1006427740013578","6084","20","17:47:48","bid"],
        ["1006427740013580","6084.8","160","17:47:48","bid"]
    ]
}]
"""
while True:
    try:
        print("Reciving...")
        # print(ws.recv())
        detail_ls = json.loads(ws.recv())[0]
        if detail_ls['channel'] == "ok_sub_futureusd_btc_trade_next_week":
            detail = detail_ls['data']
            for _ in detail:
                dic = {
                    "exchange": "Okex",
                    "measurement": "trade",
                    "onlyKey": "Okex_BTCNEXTWEEK_USD",
                    "price": _[1],
                    "side": 'buy' if _[4] == "bid" else 'sell',
                    "symbol": "BTC",
                    "timestamp": cur_time(),
                    "tradeId": _[0],
                    "unit": "USD",
                    "volume": float(_[2]) * 100 / float(_[1])
                }
                print(dic)
                producer.send('trade-dev', [dic])
                print("send successful")
    except Exception as e:
        try:
            print(e)
            print("ws重连")
            time.sleep(1)
            ws_connect()
        except Exception as e:
            print(e)
            print("重连失败，等五秒再次尝试")
            time.sleep(5)
            ws_connect()