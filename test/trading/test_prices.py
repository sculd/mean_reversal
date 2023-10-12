import unittest
import pandas as pd, numpy as np
import algo.trading.prices
from unittest.mock import MagicMock


class PricesTest(unittest.TestCase):

    def test_price_cache_empty_current_data(self):
        time_and_values = [[0, 60 * 1000, 120 * 1000], [10, 11, 12]]
        columns = ['timestamp', 'symbol1']
        df = pd.DataFrame(list(zip(*time_and_values)), columns =columns)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df = df.set_index('timestamp')
        print(df)

        price_cache = algo.trading.prices.PriceCache(['symbol1'], 4, df_prices=None, now_epoch_seconds=121)
        price_cache.fetch_closes = MagicMock(return_value=df)

        df_got = price_cache.get_df_prices()
        print(df_got)
        self.assertTrue(df.equals(df_got))


    def test_price_cache_update_data(self):
        pass

        '''
        time_and_values = [[0, 60, 120], [10, 11, 12]]
        columns = ['timestamp', 'symbol1']

        df = pd.DataFrame(list(zip(*time_and_values)), columns =columns)
        print(df)

        price_cache = algo.trading.prices.PriceCache(['symbol1'], 4, df_prices=None, now_epoch_seconds=None)

        price_cache
        thing = ProductionClass()
        thing.method = MagicMock(return_value=3)
        thing.method(3, 4, 5, key='value')
        thing.method.assert_called_with(3, 4, 5, key='value')
        '''



if __name__ == "__main__":
    unittest.main()
