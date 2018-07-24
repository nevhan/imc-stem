import json

from websocket import create_connection

test_key = '46409838-61C2-493E-873E-ADDBCDABADA3'


# test_key = 'DD0864B8-4A5C-42D0-BB42-9A1D9AF50E16'
# test_key = '1AF2DC21-D65D-494C-890B-A03C82014A07'
# test_key = 'E4A64B56-AE2D-45FD-B73E-28BA66F9E1C6'

class CoinAPIv1_subscribe(object):
    def __init__(self, apikey):
        self.type = "hello"
        self.apikey = apikey
        self.heartbeat = True
        self.subscribe_data_type = ["trade"]
        self.subscribe_filter_asset_id = ["BCH", "EOS"]
        self.subscribe_filter_symbol_id = ['BITFINEX_', 'BINANCE_']


ws = create_connection("wss://ws.coinapi.io/v1")
sub = CoinAPIv1_subscribe(test_key);
ws.send(json.dumps(sub.__dict__))

import json
import pandas as pd


def listen():
    messages = [json.loads(ws.recv()) for _ in range(100)]
    return pd.DataFrame(messages)


raw = listen()


def munge(raw):
    data = raw.copy()
    data = data.join(
        data.symbol_id.str.split('_', expand=True).rename({0: 'Source', 1: 'Kind', 2: 'Coin', 3: 'Base'}, axis=1)
    )
    data = data[data['Base'].isin(['USDT', 'USD'])]

    data.loc[:, 'time_exchange'] = pd.to_datetime(data['time_exchange'])
    data = data.set_index(['Coin', 'time_exchange']).sort_index()['price']

    data = data[~data.index.duplicated()].unstack(level='Coin').fillna(method='ffill')
    return data.dropna()


data = munge(raw)
