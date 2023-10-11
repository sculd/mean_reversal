import algo.trading.prices_csv

csv_filename = f'/home/junlim/projects/mean_reversal/algo/data/toy.csv'
#symbols = ['YFIIUSDT', 'ETHUSDT', 'BIFIUSDT']
symbols = ['symbol1', 'symbol2']
price_cache = algo.trading.prices_csv.BacktestCsvPriceCache(csv_filename, symbols, 2)

print(price_cache.get_df_prices())
print(price_cache.get_df_prices())
print(price_cache.get_df_prices())
print(price_cache.get_df_prices())
print(price_cache.get_df_prices())
