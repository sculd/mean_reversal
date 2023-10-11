import algo.trading.trade
import logging
import sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logging.basicConfig(format='%(asctime)s %(message)s')


trade_manager = algo.trading.trade.TradeManager(['BTC/USD', 'ETH/USD', 'LTC/USD'])

import algo.trading.cycle

cycle = algo.trading.cycle.Cycle(trade_manager, 10)
cycle.start()
