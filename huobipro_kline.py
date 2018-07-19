import requests
import websocket
import json
import ssl
import chardet
import gzip
import logging
from kafka import KafkaProducer

try:
    import thread
except ImportError:
    import _thread as thread
import time

# 日志设置
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s (filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='logs/huobi_kline.log',
                    filemode='a')
maps = {
    "1day": "1D",
    "1week": "1W",
    "1mon": "1M"
}


def on_message(ws, message):
    d = json.loads(gzip.decompress(message).decode('utf-8'))
    if not isinstance(d, dict):
        self.logger.error('message not dict, pass ...')
        return False

    if d.get("ping"):
        logging.info('Server pinged: {} ...'.format(d["ping"]))
        try:
            ws.send(json.dumps({
                "pong": d["ping"]
            }))
            logging.info('Client ponged: {} ...'.format(d["ping"]))
        except Exception as e:
            logging.error("Error sending pong!")
            logging.exception(e)
        return False

    if d.get("subbed"):
        logging.info("Subscribed: {} status: {}!".format(d["subbed"], d["status"]))
        return False

    if d.get("err-code"):
        logging.error('SERVER RETURNS ERROR: {}: {}'.format(d["err-code"], d.get("err-msg")))
        return False

    if not d.get("tick"):
        logging.error('Message no tick, pass ...')
        return False

    # pair = d.get("ch").split('.')[1]
    pair = d.get('ch')

    if pair not in symbols_map:
        logging.error('channel id not in map, pass ...')
        return False
    map = pair.split('.')[3]
    currency_from, currency_to, p = symbols_map[pair]
    tick = d['tick']
    dic = {
        "close": tick['close'],  # 昨收价
        "exchange": "Huobi",  # 交易所
        "high": tick['high'],  # 最高
        "low": tick['low'],  # 最低
        "measurement": "kline_%s" % maps['%s' % map],  # 来源
        "onlyKey": "Huobi_%s_%s" % (currency_from.upper(), currency_to.upper()),  # 交易对
        "open": tick['open'],  # 开盘价
        "symbol": currency_from.upper(),  # 左交易对
        "timestamp": d['ts'],  # 时间戳
        "unit": currency_to.upper(),  # 右交易对
        "volume": tick['vol']  # 数量
    }
    producer.send('kline-dev', [dic])
    logging.info("send successful >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")


def on_error(ws, error):
    print(error)


def on_close(ws):
    print("### closed ###")


def on_open(ws):
    def run(*args):
        print("ws start...")
        logging.info('ws starting ...')
        for symbol in symbols_map:
            ws.send(json.dumps({
                "sub": symbol,
                "id": 'id1'
            }))
    thread.start_new_thread(run, ())


if __name__ == "__main__":
    re = requests.get('https://api.huobi.pro/v1/common/symbols')
    symbols_map = dict()
    for _ in json.loads(re.text)['data']:
        for p in ['1day', '1week', '1mon']:
            sym = _["base-currency"] + _["quote-currency"]
            symbols_map["market.{}.kline.{}".format(sym, p)] = (_["base-currency"], _["quote-currency"], p)
    print("websocket的参数已生成")
    logging.info("websocket的参数已生成")
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://api.huobipro.com/ws",
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