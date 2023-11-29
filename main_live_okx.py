import algo.trading.trade
import algo.trading.prices_okx
import algo.trading.execution_okx
import algo.trading.cycle
import logging
import sys


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.FileHandler("logs/{}.log".format("log_live")),
        logging.StreamHandler(sys.stdout)
    ]
)



class Live:
    def __init__(self, symbols):
        self.trading_param = algo.trading.trade.StatArbitrageTradingParam.get_default_param()
        self.price_cache = algo.trading.prices_okx.PriceCache(symbols, self.trading_param.get_max_window_minutes())
        self.trade_execution = None
        #self.trade_execution = algo.trading.execution_okx.TradeExecution(symbols, target_betsize=100)
        self.trade_manager = algo.trading.trade.TradeManager(symbols, price_cache=self.price_cache, trade_execution=self.trade_execution)
        self.cycle = algo.trading.cycle.Cycle(self.trade_manager, 10)

    def run(self):
        self.cycle.start()


live = Live(['LTC-USDT-SWAP', 'BTC-USDT-SWAP'])
live.run()


