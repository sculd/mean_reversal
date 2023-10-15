import logging


class ExecutionRecord:
    def __init__(self, epoch_seconds, prices, weights, direction):
        self.epoch_seconds, self.prices, self.weights, self.direction = epoch_seconds, prices, weights, direction
        pws = zip(list(prices), weights)
        self.value = round(sum(map(lambda pw: pw[0] * pw[1], pws)), 1)


class ExecutionRecords:
    def __init__(self):
        self.records = []

    def get_pnl(self):
        direction = 0
        value = 0
        pnls = []
        cum_pnl = 0
        for record in self.records:
            if record.direction == 1:
                value = record.value
            elif record.direction == -1:
                if direction == 1:
                    pnl = record.value - value
                    pnl = round(pnl, 1)
                    pnls.append(pnl)
                    cum_pnl += pnl
            
            direction = record.direction

        logging.info(f'cum_pnl: {cum_pnl}\npnls: {pnls}')
        

class TradeExecution:
    def __init__(self, symbols):
        self.symbols = symbols
        self.direction = 0   
        self.execution_records = ExecutionRecords()
        self.valid_execution_records = ExecutionRecords()

    def execute(self, epoch_seconds, price_series, weights, direction):
        '''
        negative weight meaning short-selling.

        direction: +1 for enter, -1 for leave.
        '''
        pws = zip(list(price_series), weights)
        value = round(sum(map(lambda pw: pw[0] * pw[1], pws)), 1)
        logging.info(f'execute prices: {price_series.values}, weights: {weights}, value: {value}, direction: {direction}')
        record = ExecutionRecord(epoch_seconds, price_series.values, weights, direction)
        self.execution_records.records.append(record)
        if (direction == 1 and self.direction != 1) or (direction == -1 and self.direction == 1):
            self.valid_execution_records.records.append(record)
            if direction == -1:
                self.valid_execution_records.get_pnl()
        self.direction = direction
