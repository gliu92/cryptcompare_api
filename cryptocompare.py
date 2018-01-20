import requests
import pandas as pd
import os


class CryptoCompare:
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
                return frame
            else:
                raise Exception('No Data found in response, instead received: {}'.format(r.json()))
        else:
            raise Exception('Received error with status code {} when requesting for data'.format(r.status_code))