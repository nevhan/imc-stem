import pandas as pd
from bitex.api.WSS import BitfinexWSS

wss = None


def get_latest_update():
    raw = [wss.data_q.get() for n in range(wss.data_q.qsize())]
    return [m for m in raw if m[0] == 'trades']


def munge(messages):

    def message_to_series(msg):
        keys = ('trade_id', 'timestamp', 'size', 'price')

        for _, symbol, grp in msg:
            trades, ts = grp
            if isinstance(trades[0], list):
                for trd in trades[0]:
                    yield dict(**dict(zip(keys, trd)), symbol=symbol)
            else:
                yield dict(**dict(zip(keys, trades[1])), symbol=symbol)

    df = pd.DataFrame([x for x in message_to_series(messages)])
    df['timestamp'] = pd.DatetimeIndex(df['timestamp'] * 1000000).tz_localize('UTC').tz_convert('Australia/Sydney')
    df = df.set_index(['symbol', 'timestamp']).sort_index().astype({'price': float, 'size': float}).drop_duplicates()
    df = df['price'].unstack(level='symbol').fillna(method='ffill').dropna()
    return df


def get_latest_trade_ticks():
    messages = get_latest_update()
    if messages:
        df = munge(messages)
        return df


def connect_ws(pairs):
    global wss
    wss = BitfinexWSS(pairs=pairs)
    wss.start()
