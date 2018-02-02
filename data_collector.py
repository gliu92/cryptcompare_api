import pandas as pd
from cryptocompare import CryptoCompare
import os
import sys
import time
import logging


DATA_DIR = 'data/'

cc = CryptoCompare()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s|%(levelname)s|%(message)s',
                    datefmt='%Y%m%d %H:%M:%S',
                    stream=sys.stdout)

# Get list of coins on binance
EXCHANGES = ['Binance', 'Bitfinex', 'Coinbase']
BASE_SYMS = ['BTC', 'USD', 'USDT', 'ETH']
SLEEP_TIME_S = 60

for exchange in EXCHANGES:
    for base_sym in BASE_SYMS:
        coin_pairs = cc.coins_by_exchange(exchange)[base_sym]

        for fsym, sym in coin_pairs.items():
            prices = cc.get_data(sym, exchange, bar_size='minute')
            fpath = os.path.join(DATA_DIR, '{}_{}.csv'.format(exchange.lower(), sym.lower().replace('/', '')))
            if prices is not None:
                if os.path.exists(fpath):
                    try:
                        data = pd.read_csv(fpath, index_col=0)
                        if len(data):
                            prev_timestamp = data.index[-1]
                            prices_subframe = prices.loc[prev_timestamp:].iloc[1:]
                            if len(prices_subframe):
                                logger.info('Processing {}:{} {} {}'.format(exchange, sym, prev_timestamp, prices_subframe.index[0]))
                                df = pd.concat([data, prices_subframe])
                                df.to_csv(fpath)
                        else:
                            prices.to_csv(fpath)
                    except:
                        logger.warning('Existing file for {}:{} is corrupted, overriding csv.'.format(exchange, sym))
                        prices.to_csv(fpath)

                else:
                    prices.to_csv(fpath)

        time.sleep(SLEEP_TIME_S)
        logger.info('Finished base symbol {}. Sleeping...{}s'.format(base_sym, SLEEP_TIME_S))
    time.sleep(SLEEP_TIME_S)
    logger.info('Finished exchange {}. Sleeping...{}s'.format(exchange, SLEEP_TIME_S))
