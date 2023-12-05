import logging, os, requests
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

def get_usdt_symbol(symbol):
    symbol_usdt = symbol
    if symbol_usdt.endswith('USD'):
        symbol_usdt = symbol.replace('USD', 'USDT')
    return symbol_usdt

def get_current_price(symbol):
    candle_hist_url_format = 'https://www.okx.com/api/v5/market/history-mark-price-candles?bar=1m&instId={symbol}&limit={limit}'

    symbol_usdt = get_usdt_symbol(symbol)
    url = candle_hist_url_format.format(symbol=symbol_usdt, limit=1)
    r = requests.get(url)
    r_js = r.json()
    price = float(r_js['data'][0][4])
    return price


class TradeExecution:
    def __init__(self, symbols, target_betsize, leverage):
        self.symbols = symbols
        self.target_betsize = target_betsize
        self.direction = 0   
        self.execution_records = algo.trading.execution.ExecutionRecords()
        self.closed_execution_records = algo.trading.execution.ClosedExecutionRecords()
        self.init_inst_data()
        self.close_open_positions()
        self.setup_leverage(symbols, leverage)
        self.ensure_collateral_coins()

    def init_inst_data(self):
        public_data_api = PublicData.PublicAPI(flag=_flag)
        get_instruments_result = public_data_api.get_instruments(
            instType="SWAP"
        )
        self.inst_data = {instData['instId']: instData for instData in get_instruments_result['data']}
        get_instruments_result = public_data_api.get_instruments(
            instType="SPOT"
        )
        self.inst_data_spot = {instData['instId']: instData for instData in get_instruments_result['data']}

    def setup_leverage(self, symbols, leverage):
        account_api = get_account_api()
        self.leverage = int(leverage)
        for symbol in symbols:
            logging.info(f'setting up the leverage for {symbol} as {leverage}')
            result = account_api.set_leverage(
                instId = symbol,
                lever = str(self.leverage),
                mgnMode = "isolated"
            )
            print(result)

    def close_open_positions(self):
        account_api = get_account_api()
        trade_api = get_trade_api()
        positions_result = account_api.get_positions()
        for position in positions_result['data']:
            logging.info(f"closing position {position['instId']} {position['posSide']}")
            close_result = trade_api.close_positions(position['instId'], 'isolated', posSide=position['posSide'], ccy='')
            logging.info(close_result)

    def ensure_collateral_coins(self):
        for symbol in self.symbols:
            self.ensure_collateral_coin(symbol)

    def ensure_collateral_coin(self, symbol):
        account_api = get_account_api()
        account_balance_result = account_api.get_account_balance()

        target_holding_usd = (self.target_betsize + 20) / self.leverage

        coin = symbol.split('-USD')[0]
        balance_detail = None
        for d in account_balance_result['data'][0]['details']:
            if d['ccy'] == coin:
                balance_detail = d
                break

        current_holding_usd = 0
        current_holding = 0
        if balance_detail is not None:
            current_holding_usd = float(balance_detail['eqUsd'])
            current_holding = float(balance_detail['availBal'])
        
        collateral_symbol = symbol.replace('-SWAP', '')
        collateral_symbol = get_usdt_symbol(collateral_symbol)
        current_price = get_current_price(collateral_symbol)
        target_holding = target_holding_usd / current_price

        inst_data_spot = self.inst_data_spot[collateral_symbol]
        lot_sz = float(inst_data_spot['lotSz'])

        holding_delta_usd = target_holding_usd - current_holding_usd
        holding_delta_usd = int(holding_delta_usd)
        holding_delta = target_holding - current_holding

        logging.info(f'[ensure_collatral_coin] securing {coin} (trade {collateral_symbol}) (current_price: {current_price}) for {symbol}, target_holding_usd: {target_holding_usd} (current_holding_usd: {current_holding_usd}), target_holding: {target_holding} (current_holding: {current_holding}), holding_delta_usd: {holding_delta_usd}, lot_sz: {lot_sz}')
        
        if abs(holding_delta_usd) < 2:
            logging.info(f'collateral {collateral_symbol} is already secured')
            return
        
        trade_api = get_trade_api()
        if holding_delta_usd > 0:
            result = trade_api.place_order(
                instId=collateral_symbol, tdMode="cash", 
                side="buy",
                posSide="net",
                ordType="market",
                sz=str(holding_delta_usd),
            )
        else:
            result = trade_api.place_order(
                instId=collateral_symbol, tdMode="cash", 
                side="sell",
                posSide="net",
                ordType="market",
                sz=str(abs(holding_delta)),
            )

        logging.info(f'place order result:\n{result}')

        if result["code"] == "0":
            logging.info(f'Successful order request, order_id: {result["data"][0]["ordId"]}')
        else:
            logging.error(f'Unsuccessful order request, error_code = {result["data"][0]["sCode"]}, Error_message = {result["data"][0]["sMsg"]}')

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
        logging.info(f'at {epoch_seconds}, for {self.symbols}, execute prices: {price_series.values}, weights: {weights}, value: {value}, direction: {direction}, self.direction: {self.direction}')
        record = algo.trading.execution.ExecutionRecord(epoch_seconds, self.symbols, price_series.values, weights, direction)
        self.execution_records.append_record(record)

        if direction == 1 and self.direction != 1:
            sz_factor = self.get_size_factor(price_series, weights)

            sws = list(zip(list(self.symbols), weights))
            for sw in sws:
                symbol, weight = sw
                trade_api = get_trade_api()
                
                lot_sz = float(self.inst_data[symbol]['lotSz'])
                sz_target = weight * sz_factor / lot_sz
                sz = int(sz_target)
                logging.info(f'for {symbol}, target sz: {sz_target}, actual sz: {sz}, delta: {sz - sz_target}, lot_sz: {lot_sz}')
                
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
                    logging.error(f'Unsuccessful order request, error_code = {result["data"][0]["sCode"]}, Error_message = {result["data"][0]["sMsg"]}')

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
                    logging.error(f'Can not find the position for {symbol}, something is wrong.')
                    continue

                result = trade_api.close_positions(
                    symbol, 'isolated', 
                    posSide=position_data['posSide'], ccy='')
                logging.info(f'close order result:\n{result}')

                if result["code"] == "0":
                    logging.info("Successful order close request")
                else:
                    logging.error(f"Unsuccessful order request {result}")

        self.direction = direction


    def get_out_of_current_position(self, epoch_seconds, price_series, weights):
        if self.direction != 1:
            return
        logging.info(f'at {epoch_seconds}, get_out_of_current_position prices: {price_series.values}, weights: {weights}')
        self.execute(epoch_seconds, price_series, weights, -1)

    def print(self):
        logging.info(f'[Okx TradeExecution] symbols: {self.symbols}, betsize: {self.target_betsize}, direction: {self.direction}')
        self.execution_records.print()
        self.closed_execution_records.print()
        logging.info(f'closed trades pairs: {len(self.closed_execution_records.closed_records)}, cum_pnl: {self.closed_execution_records.get_cum_pnl()}')
