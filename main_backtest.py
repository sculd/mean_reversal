import pandas as pd
import algo.trading.trade
import algo.trading.prices_csv
import logging
import sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s %(message)s')

class Backtest:
    def __init__(self, csv_filename, symbols):
        self.trading_param = algo.trading.trade.TradingParam.get_default_param(symbols)
        self.price_cache = algo.trading.prices_csv.BacktestCsvPriceCache(csv_filename, symbols, self.trading_param.get_max_window_minutes())
        self.price_cache.warmup()
        self.trade_manager = algo.trading.trade.TradeManager(symbols, price_cache=self.price_cache)


    def run(self):
        cnt = 0
        while True:
            self.trade_manager.tick()
            if self.price_cache.if_history_all_read():
                break

            cnt += 1
            if cnt % 100 == 0:
                print(cnt)

        print('done')



#csv_filename = f'algo/data/market_data_binance.by_minute_ALL_2022-09-01T04:00:00Z_2022-09-30T03:59:00Z.csv'
csv_filename = f'algo/data/med.csv'
backtest = Backtest(csv_filename, ['YFIIUSDT', 'ETHUSDT', 'BIFIUSDT'])
backtest.run()
