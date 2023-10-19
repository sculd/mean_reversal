import threading, time
import logging


def on_cycle(trade_manager):
    logging.debug(time.ctime())
    trade_manager.tick()


class CycleTimer(threading.Timer):  
    def run(self):  
        while not self.finished.wait(self.interval):  
            self.function(*self.args,**self.kwargs)


class Cycle:
    def __init__(self, trade_manager, cycle_period_seconds):
        self.cycle_timer = CycleTimer(cycle_period_seconds, on_cycle, [trade_manager])

    def start(self):
        self.cycle_timer.start()

    def cancel(self):
        self.cycle_timer.cancel()
