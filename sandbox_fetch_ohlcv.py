import datetime, time
import pandas as pd, numpy as np
import ccxt

import algo.trading.prices


binance = ccxt.binance()
gemini = ccxt.gemini()
kraken = ccxt.kraken()



df = algo.trading.prices.fetch_closes(kraken, ['BTC/USD', 'ETH/USD', 'LTC/USD'], 4)
print(df)




price_cache = algo.trading.prices.PriceCache(['BTC/USD', 'ETH/USD', 'LTC/USD'], 4, df.iloc[:-2])

df = price_cache.get_df_prices()
print(df)


