import numpy as np
import datetime, logging


class ExecutionRecord:
    def __init__(self, epoch_seconds, prices, weights, direction):
        self.epoch_seconds, self.prices, self.weights, self.direction = epoch_seconds, prices, weights, direction
        self.value = round(sum(map(lambda pw: pw[0] * pw[1], zip(list(prices), weights))), 1)
        self.magnitude = round(sum(map(lambda pw: pw[0] * abs(pw[1]), zip(list(prices), weights))), 1)
    
    def __str__(self):
        return f'at {datetime.datetime.fromtimestamp(self.epoch_seconds)}, prices: {self.prices}, weights: {self.weights}, value: {self.value}, magnitude: {self.magnitude}, direction: {self.direction}'

    def print(self):
        logging.info(str(self))


class ClosedExecutionRecord:
    def __init__(self, record_enter, record_exit):
        self.record_enter, self.record_exit = record_enter, record_exit
    
    def get_pnl(self):
        pnl = self.record_exit.value - self.record_enter.value
        pnl = round(pnl, 1)
        return pnl

    def __str__(self):
        #return f'enter at {datetime.datetime.fromtimestamp(self.record_enter.epoch_seconds)}, exit at {datetime.datetime.fromtimestamp(self.record_exit.epoch_seconds)}, duration: {int(self.record_exit.epoch_seconds - self.record_enter.epoch_seconds / 60)} minutes, pnl: {self.get_pnl()}'
        return f'enter {self.record_enter}\nexit {self.record_exit}\nduration: {int((self.record_exit.epoch_seconds - self.record_enter.epoch_seconds) / 60)} minutes, pnl: {self.get_pnl()}'

    def print(self):
        logging.info(str(self))


class ClosedExecutionRecords:
    def __init__(self):
        self.closed_records = []
        self.enter_record = None

    def enter(self, enter_record):
        self.enter_record = enter_record

    def get_cum_pnl(self):
        cum_pnl = 0
        for closed_record in self.closed_records:
            pnl = closed_record.get_pnl()
            if np.isnan(pnl):
                continue
            cum_pnl += pnl

        return cum_pnl

    def print(self):
        for closed_record in self.closed_records:
            closed_record.print()


class ExecutionRecords:
    def __init__(self):
        self.records = []

    def append_record(self, record):
        self.records.append(record)

    def get_cum_pnl(self):
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
                    if np.isnan(pnl):
                        continue
                    pnls.append(pnl)
                    cum_pnl += pnl
            
            direction = record.direction

        return cum_pnl

    def print(self):
        for record in self.records:
            record.print()


class TradeExecution:
    def __init__(self, symbols):
        self.symbols = symbols
        self.direction = 0   
        self.execution_records = ExecutionRecords()
        self.closed_execution_records = ClosedExecutionRecords()

    def execute(self, epoch_seconds, price_series, weights, direction):
        '''
        negative weight meaning short-selling.

        direction: +1 for enter, -1 for leave.
        '''
        pws = zip(list(price_series), weights)
        value = round(sum(map(lambda pw: pw[0] * pw[1], pws)), 3)
        logging.info(f'at {epoch_seconds}, execute prices: {price_series.values}, weights: {weights}, value: {value}, direction: {direction}')
        record = ExecutionRecord(epoch_seconds, price_series.values, weights, direction)
        self.execution_records.append_record(record)

        if direction == 1 and self.direction != 1:
            self.closed_execution_records.enter(record)

        if direction == -1 and self.direction == 1:
            closed_record = ClosedExecutionRecord(self.closed_execution_records.enter_record, record)
            self.closed_execution_records.closed_records.append(closed_record)
            logging.info(f'closed: {closed_record} trades pairs: {len(self.closed_execution_records.closed_records)}, cum_pnl: {self.closed_execution_records.get_cum_pnl()}')

        self.direction = direction

    def get_out_of_current_position(self, epoch_seconds, price_series, weights):
        if self.direction != 1:
            return
        logging.info(f'at {epoch_seconds}, get_out_of_current_position prices: {price_series.values}, weights: {weights}')
        self.execute(epoch_seconds, price_series, weights, -1)


    def print(self):
        self.closed_execution_records.print()
        logging.info(f'closed trades pairs: {len(self.closed_execution_records.closed_records)}, cum_pnl: {self.closed_execution_records.get_cum_pnl()}')
