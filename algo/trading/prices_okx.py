import pandas as pd, numpy as np
import datetime, time
import logging

import websocket

_ws_address = 'wss://ws.okx.com:8443/ws/v5/business'

import ssl
import json

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')
from google.cloud import bigquery
import pytz
from threading import Thread


def _get_epoch_seconds_before(minutes_before):
    now_epoch_seconds = int(time.time() // 60) * 60
    return (now_epoch_seconds - minutes_before * 60)


def _get_epoch_millis_before(minutes_before):
    return _get_epoch_seconds_before(minutes_before) * 1000

def epoch_seconds_to_datetime(timestamp_seconds):
    t = datetime.datetime.utcfromtimestamp(timestamp_seconds)
    t_tz = pytz.utc.localize(t)
    return t_tz




def _message_to_bwt_dict(symbol, data_entry):
    '''
    {"arg":{"channel":"candle1m","instId":"XCH-USDT-SWAP"},"data":[["1700931960000","26.2","26.2","26.2","26.2","307","3.07","80.434","0"]]}
    '''
    if not data_entry:
        logging.error('the message is empty')
    epoch_milli = int(data_entry[0])
    epoch_seconds = epoch_milli // 1000
    open_, high, low, close_ = float(data_entry[1]), float(data_entry[2]), float(data_entry[3]), float(data_entry[4])
    # 5 is not the volume
    volume = float(data_entry[6])

    return {
        "market": 'okx',
        "symbol": symbol,
        "open": open_,
        "high": high,
        "low": low,
        "close": close_,
        "volume": volume,
        "epoch_seconds": epoch_seconds
    }


def _message_to_bwt_dicts(symbol, data):
    if not data:
        logging.error('the message is empty')

    return [_message_to_bwt_dict(symbol, data_entry) for data_entry in data]


_msg_cnt = 0

class PriceCache:
    def __init__(self, symbols, windows_minutes, df_prices=None, now_epoch_seconds=None):
        '''
        df_prices, now_epoch_seconds are for unit test.
        '''
        self.symbols = symbols
        self.windows_minutes = windows_minutes
        self.df_prices = df_prices

        self.bigquery_client = bigquery.Client()
        if self.df_prices is None:
            self.df_prices = self.fetch_closes(symbols, windows_minutes)
            print(f'self.df_prices\n{self.df_prices}')

        self.dict_recent_prices = {symbol: None for symbol in symbols}
        self.recent_epoch_seconds = 0
        self.now_epoch_seconds = now_epoch_seconds

        ws = websocket.WebSocketApp(_ws_address, on_open = self.on_ws_open, on_close = self.on_ws_close, on_message = self.on_ws_message, on_error = self.on_ws_error)
        t = Thread(target=ws.run_forever, kwargs={"sslopt": {"cert_reqs": ssl.CERT_NONE}})
        t.daemon = True
        t.start()

    def on_ws_open(self, ws):
        print('Opened Connection')
        channels = [{"channel": "candle1m", "instId": symbol} for symbol in self.symbols]
        params = {
            "op": "subscribe",
            "args": channels,
        }
        ws.send(json.dumps(params))

    def on_ws_close(self, ws, *args):
        print(f'Closed Connection {args}')

    def on_ws_message(self, ws, msg):
        global _msg_cnt
        '''
        {"arg":{"channel":"candle1m","instId":"XCH-USDT-SWAP"},"data":[["1700931960000","26.2","26.2","26.2","26.2","307","3.07","80.434","0"]]}
        '''
        msg_js = json.loads(msg)
        if 'data' not in msg_js:
            print(f'{msg} is not candle msg, skipping')
            return

        msg_data = msg_js['data']
        
        _msg_cnt += 1
        if _msg_cnt % 5000 == 0:
            print("{msg}".format(msg=msg))

        symbol = msg_js['arg']['instId']
        bwt_dicts = _message_to_bwt_dicts(symbol, msg_data)

        for bwt_dict in bwt_dicts:
            if self.recent_epoch_seconds == bwt_dict['epoch_seconds']:
                self.dict_recent_prices[symbol] = bwt_dict['close']
                print(self.dict_recent_prices)
            elif self.recent_epoch_seconds < bwt_dict['epoch_seconds']:
                if self.recent_epoch_seconds > 0:
                    self.df_prices = self.concat_recent_prices()
                    print(f'self.df_prices\n{self.df_prices}')

                self.recent_epoch_seconds = bwt_dict['epoch_seconds']


    def concat_recent_prices(self):
        df_recent_prices = pd.DataFrame.from_dict({'timestamp': [self.recent_epoch_seconds], **{k: [v] for k, v in self.dict_recent_prices.items()}}).set_index('timestamp')
        df_recent_prices.index = pd.to_datetime(df_recent_prices.index, unit='s', utc=True)

        if self.df_prices is None:
            return df_recent_prices
        
        df_prices = pd.concat([self.df_prices, df_recent_prices])
        df_prices = df_prices[max(0, len(self.df_prices) - self.windows_minutes):]
        return df_prices


    def on_ws_error(self, ws, err):
        print("Got a an error: ", err)

    
    def get_now_epoch_seconds(self):
        if self.now_epoch_seconds is not None:
            return self.now_epoch_seconds
        return int(time.time())

    def fetch_closes_since_for_symbol(self, symbol, since_epoch_seconds):
        logging.debug(f'fetching prices for {symbol} since {since_epoch_seconds}({datetime.datetime.fromtimestamp(since_epoch_seconds)})')

        t_since = epoch_seconds_to_datetime(since_epoch_seconds)
        t_str_since = t_since.strftime("%Y-%m-%d %H:%M:%S %Z")

        query = f"""
            WITH LATEST AS (
            SELECT timestamp, max(ingestion_timestamp) AS max_ingestion_timestamp
            FROM `trading-290017.market_data_okx.by_minute` 
            WHERE TRUE
            AND timestamp >= "{t_str_since}"
            AND symbol = "{symbol}"
            GROUP BY timestamp
            )

            SELECT *
            FROM `trading-290017.market_data_okx.by_minute` AS T JOIN LATEST ON T.timestamp = LATEST.timestamp AND T.ingestion_timestamp = LATEST.max_ingestion_timestamp
            WHERE TRUE
            AND T.timestamp >= "{t_str_since}"
            AND T.symbol = "{symbol}"
            ORDER BY T.timestamp DESC
        """
        print(f'query:\n{query}')
        
        rows_data = {symbol: [], "timestamp": []}
        bq_query_job = self.bigquery_client.query(query)
        for row in bq_query_job:
            rows_data[symbol].append(row['close'])
            rows_data["timestamp"].append(row["timestamp"])

        df = pd.DataFrame.from_dict(rows_data).set_index('timestamp')
        df.index = pd.to_datetime(df.index, unit='s', utc=True)

        logging.debug(f'fetched {len(df)} rows')
        return df

    def fetch_closes_since(self, symbols, since_epoch_seconds):
        logging.debug(f'fetching prices for {symbols} since {since_epoch_seconds}({datetime.datetime.fromtimestamp(since_epoch_seconds)})')

        df = None
        for symbol in symbols:
            df_symbol = self.fetch_closes_since_for_symbol(symbol, since_epoch_seconds)
            if df is None:
                df = df_symbol
            else:
                df = df.join(df_symbol).dropna()

        logging.debug(f'fetched {len(df)} rows')
        return df

    def fetch_closes(self, symbols, window_minutes):
        logging.info(f'fetching prices for {symbols} for last {window_minutes} minutes')
        epoch_seconds_first = _get_epoch_seconds_before(window_minutes)
        df = self.fetch_closes_since(symbols, epoch_seconds_first)
        logging.info(f'fetched {len(df)} rows')
        return df

    def get_df_prices(self):
        return self.concat_recent_prices()




