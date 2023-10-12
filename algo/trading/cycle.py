import threading, time
import logging


class Cycle:
    def __init__(self, trade_manager, cycle_period_seconds):
        self.trade_manager = trade_manager
        self.cycle_period_seconds = cycle_period_seconds

    def start(self):
        ticker = threading.Event()
        while not ticker.wait(self.cycle_period_seconds):
            self.on_cycle()

    def on_cycle(self):
        logging.debug(time.ctime())
        self.trade_manager.tick()
