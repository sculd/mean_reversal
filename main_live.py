import algo.trading.trade
import algo.trading.prices
import algo.trading.cycle
import ccxt
import logging
import sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s %(message)s')

class Live:
    def __init__(self, symbols, exchange):
        self.trading_param = algo.trading.trade.TradingParam.get_default_param(symbols)
        self.price_cache = algo.trading.prices.PriceCache(symbols, self.trading_param.get_max_window_minutes(), exchange=exchange)
        self.trade_manager = algo.trading.trade.TradeManager(symbols, price_cache=self.price_cache)
        self.cycle = algo.trading.cycle.Cycle(self.trade_manager, 10)

    def run(self):
        self.cycle.start()


_exchange_gemini = ccxt.gemini()

live = Live(['BTC/USD', 'ETH/USD', 'LTC/USD'], exchange=_exchange_gemini)
live.run()
