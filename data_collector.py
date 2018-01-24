import pandas as pd
from cryptocompare import CryptoCompare
import os

DATA_DIR = 'data/'

cc = CryptoCompare()

# Get list of coins on binance
exchange = 'Binance'
coin_pairs = cc.coins_by_exchange(exchange)
btc_pairs = coin_pairs['BTC']

for fsym, sym in btc_pairs.items():
    prices = cc.get_data(sym, exchange, bar_size='minute')
    fpath = os.path.join(DATA_DIR, sym.lower().replace('/', '') + '.csv')
    if prices is not None:
        if os.path.exists(fpath):
            data = pd.read_csv(fpath, index_col=0)
            if len(data):
                prev_timestamp = data.index[-1]
                prices_subframe = prices.loc[prev_timestamp:].iloc[1:]
                if len(prices_subframe):
                    print(sym, prev_timestamp, prices_subframe.index[0])
                    df = pd.concat([data, prices_subframe])
                    df.to_csv(fpath)
            else:
                prices.to_csv(fpath)

        else:
            prices.to_csv(fpath)