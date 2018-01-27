import requests
import pandas as pd
import os
import collections
import logging

logger = logging.getLogger(__name__)


class CryptoCompare:
    """
    API doc: https://min-api.cryptocompare.com/
    """

    URL = 'https://min-api.cryptocompare.com/'
    TIMEZONE = 'US/Central'

    def get_data(self, symbol, exchange, limit=2000, window=1, bar_size='day', **kwds):
        url = os.path.join(self.URL, 'data', 'histo{}'.format(bar_size))
        fsym, tsym = symbol.split('/')
        parameters = {'fsym': fsym, 'tsym': tsym, 'e': exchange, 'limit': limit, 'aggregate': window}
        parameters.update(kwds)
        r = requests.get(url, params=parameters)
        if r.status_code == 200:
            data = r.json().get('Data')
            if data:
                frame = pd.DataFrame(data)
                frame['time'] = frame['time'].map(lambda x: pd.Timestamp(x, unit='s', tz=self.TIMEZONE))
                frame.set_index('time', inplace=True)
                return frame
            else:
                logger.warning('Failed to download data for {}:{}, instead received: {}'.format(exchange, symbol, r.json()))
        else:
            logger.warning('Failed to download data for {}:{}. Received error with status code {} when requesting for data'.format(exchange, symbol, r.status_code))

        return None

    def coins_by_exchange(self, exchange):
        url = os.path.join(self.URL, 'data', 'all', 'exchanges')
        coins = requests.get(url).json().get(exchange)
        coins_by_base = collections.defaultdict(dict)
        for fsym, tsyms in coins.items():
            for tsym in tsyms:
                coins_by_base[tsym][fsym] = fsym + '/' + tsym
        return coins_by_base

    def get_exchange_list(self, fsym, tsym, limit=50):
        url = os.path.join(self.URL, 'data', 'top', 'exchanges')
        parameters = {'fsym': fsym, 'tsym': tsym, 'limit': limit}
        r = requests.get(url, params=parameters)
        if r.status_code == 200:
            data = r.json().get('Data')
            if data:
                frame = pd.DataFrame(data)
                return frame
            else:
                logger.warning('No data found')
        else:
            logger.warning('Encountered error while requesting data')

