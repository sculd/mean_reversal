import logging, os
import algo.trading.execution


_flag = "0"  # live trading: 0, demo trading: 1
_api_key = os.environ['OKX_API_KEY']
_secret_key = os.environ['OKX_SECRET_KEY']
_passphrase = os.environ['OKX_PASSPHRASE']

import okx.Trade as Trade
import okx.Account as Account
import okx.PublicData as PublicData

_trade_api = None
_account_api = None

def get_trade_api():
    global _trade_api
    if _trade_api is None:
        _trade_api = Trade.TradeAPI(_api_key, _secret_key, _passphrase, False, _flag)
    return _trade_api

def get_account_api():
    global _account_api
    if _account_api is None:
        _account_api = Account.AccountAPI(_api_key, _secret_key, _passphrase, False, _flag)
    return _account_api


class TradeExecution:
    def __init__(self, symbols, target_betsize):
        self.symbols = symbols
        self.target_betsize = target_betsize
        self.direction = 0   
        self.execution_records = algo.trading.execution.ExecutionRecords()
        self.closed_execution_records = algo.trading.execution.ClosedExecutionRecords()
        self.init_inst_data()

    def init_inst_data(self):
        public_data_api = PublicData.PublicAPI(flag=_flag)
        get_instruments_result = public_data_api.get_instruments(
            instType="SWAP"
        )
        self.inst_data = {instData['instId']: instData for instData in get_instruments_result['data']}

    def get_size_factor(self, price_series, weights):
        pws = zip(list(price_series), weights)
        betsize = 0
        for pw in pws:
            price, weight = pw
            betsize += abs(price * weight)
        
        sz_factor = self.target_betsize / betsize
        return sz_factor


    def execute(self, epoch_seconds, price_series, weights, direction):
        '''
        negative weight meaning short-selling.

        direction: +1 for enter, -1 for leave.
        '''
        pws = zip(list(price_series), weights)
        value = round(sum(map(lambda pw: pw[0] * pw[1], pws)), 3)
        logging.info(f'at {epoch_seconds}, for {self.symbols}, execute prices: {price_series.values}, weights: {weights}, value: {value}, direction: {direction}')
        record = algo.trading.execution.ExecutionRecord(epoch_seconds, self.symbols, price_series.values, weights, direction)
        self.execution_records.append_record(record)

        if direction == 1 and self.direction != 1:
            sz_factor = self.get_size_factor(price_series, weights)

            sws = list(zip(list(self.symbols), weights))
            for sw in sws:
                symbol, weight = sw
                trade_api = get_trade_api()
                
                ct_val = float(self.inst_data[symbol]['ctVal'])
                sz_target = weight * sz_factor / ct_val
                sz = int(sz_target)
                logging.info(f'for {symbol}, target sz: {sz_target}, actual sz: {sz}, delta: {sz - sz_target}')
                
                result = trade_api.place_order(
                    instId=symbol, tdMode="isolated", 
                    side="buy" if weight >= 0 else "sell",
                    posSide="long" if weight >= 0 else "short",
                    ordType="market",
                    # multiple of ctVal instrument property
                    sz=str(abs(sz)),
                )
                logging.info(f'place order result:\n{result}')

                if result["code"] == "0":
                    logging.info(f'Successful order request, order_id: {result["data"][0]["ordId"]}')
                else:
                    logging.info(f'Unsuccessful order request, error_code = {result["data"][0]["sCode"]}, Error_message = {result["data"][0]["sMsg"]}')

            self.closed_execution_records.enter(record)

        if direction == -1 and self.direction == 1:
            closed_record = algo.trading.execution.ClosedExecutionRecord(self.closed_execution_records.enter_record, record)
            self.closed_execution_records.closed_records.append(closed_record)
            logging.info(f'at {epoch_seconds}, for {self.symbols}, closed: {closed_record}, trades pairs: {len(self.closed_execution_records.closed_records)}, cum_pnl: {self.closed_execution_records.get_cum_pnl()}')

            trade_api = get_trade_api()
            account_api = get_account_api()
            positions_data = account_api.get_positions()['data']
            
            for symbol in self.symbols:
                position_data = None
                for d in positions_data:
                    if d['instId'] == symbol:
                        position_data = d
                        break
                
                if position_data is None:
                    logging.info(f'Can not find the position for {symbol}, something is wrong.')
                    continue

                result = trade_api.close_positions(
                    symbol, 'isolated', 
                    posSide=position_data['posSide'], ccy='')
                logging.info(f'close order result:\n{result}')

                if result["code"] == "0":
                    logging.info("Successful order close request")
                else:
                    logging.info(f"Unsuccessful order request {result}")

        self.direction = direction
