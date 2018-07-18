a = 'market.btcusdt.kline.1min'
b = a.replace('market.', '').replace('.kline.1min', '')
print(b)


'''
{'ch': 'market.btcusdt.kline.1min', 
 'ts': 1531883104111, 
 'tick': 
    {'id': 1531883100, 
     'open': 7437.39, 
     'close': 7437.35, 
     'low': 7436.64, 
     'high': 7437.42, 
     'amount': 2.3205, 
     'vol': 17258.242105, 
     'count': 26
     }
 }
'''