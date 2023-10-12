import logging

class TradeExecution:
    def __init__(self, symbols):
        self.symbols = symbols

    def execute(self, weights, direction):
        '''
        negative weight meaning short-selling.

        direction: +1 for enter, -1 for leave.
        '''
        logging.info(f'execute weights: {weights}, direction: {direction}')
