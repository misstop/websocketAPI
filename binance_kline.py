import requests
import websocket
import json
import ssl
import logging
from kafka import KafkaProducer

try:
    import thread
except ImportError:
    import _thread as thread
import time

# 日志设置
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s (filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='binance_kline.log',
                    filemode='a')


def on_message(ws, message):
    d = json.loads(message)
    if not isinstance(d, dict):
        logging.error('message not dict, pass ...')
        return False

    if not d.get("stream") or not d.get("data"):
        logging.error('data type error, pass ...')
        return False
    currency_from, currency_to = streams_map[d['stream']]
    data_k = d['data']['k']
    dic = {
        "close": data_k['c'],  # 昨收价
        "exchange": "Binance",  # 交易所
        "high": data_k['h'],  # 最高
        "low": data_k['l'],  # 最低
        "measurement": "kline_1D",  # 来源
        "onlyKey": "Binance_%s_%s" % (currency_from, currency_to),  # 交易对
        "open": data_k['o'],  # 开盘价
        "symbol": currency_from,  # 左交易对
        "timestamp": data_k['t'],  # 时间戳
        "unit": currency_to,  # 右交易对
        "volume": data_k['q']  # 数量
    }
    producer.send('kline-test', [dic])
    logging.info("send successful >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")


def on_error(ws, error):
    logging.info(error)
    ws.close()


def on_close(ws):
    logging.info("### closed ###")
    ws.on_open = on_open
    ws.run_forever(
        # http_proxy_host="localhost", http_proxy_port=1080,
        sslopt={"cert_reqs": ssl.CERT_NONE})


def on_open(ws):
    def run(*args):
        # ws.send(json.dumps({"sub": "market.btcusdt.kline.1min", "id": "idd"}))
        # ws.send(json.dumps({"ping": 18212558000}))
        logging.info("ws start...")
    thread.start_new_thread(run, ())


if __name__ == "__main__":
    re = requests.get('https://www.binance.com/exchange/public/product')
    symbols_map = {_["symbol"]: _ for _ in json.loads(re.text)['data']}
    print("symbols_maps 已获取")
    streams_map = dict()
    for symbol, symbol_info in symbols_map.items():
        name = "{}@kline_1d".format(symbol.lower())
        streams_map[name] = (symbol_info["baseAsset"], symbol_info["quoteAsset"])
    # 生成stream的参数
    stream_names_str = "/".join([_ for _ in streams_map])
    print("websocket的参数已生成")
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://stream.binance.com:9443/stream?streams={}".format(stream_names_str),
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close
                                )
    # kafka连接
    producer = KafkaProducer(bootstrap_servers='47.75.116.175:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    print('kafka已连接')
    ws.on_open = on_open
    ws.run_forever(
        # http_proxy_host="localhost", http_proxy_port=1080,
        sslopt={"cert_reqs": ssl.CERT_NONE})