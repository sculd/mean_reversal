import pandas as pd
import algo.trading.trade
import algo.trading.prices_csv
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.FileHandler("logs/{}.log".format("log_backtest")),
        logging.StreamHandler(sys.stdout)
    ]
)

class Backtest:
    def __init__(self, csv_filename, symbols):
        self.trading_param = algo.trading.trade.TradingParam.get_default_param()
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

        self.trade_manager.trade_execution.print()
        print('done')



#csv_filename = f'algo/data/binance/df_binance_20230904_05.csv'
csv_filename = f'algo/data/binance/df_binance_202309.csv'
backtest = Backtest(csv_filename, ['FARMUSDT', 'COMPUSDT'])
csv_filename = f'algo/data/med.csv'
csv_filename2 = f'algo/data/med2.csv'
#backtest = Backtest(csv_filename, ['BETHUSDT', 'ETHUSDT', 'YFIIUSDT'])
#backtest = Backtest(csv_filename, ['SOLUSDT', 'ILVUSDT'])
#backtest = Backtest(csv_filename2, ['BNBUPUSDT', 'BNBUSDT', 'SOLUSDT'])
backtest.run()
