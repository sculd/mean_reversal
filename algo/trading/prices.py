import pandas as pd, numpy as np
import ccxt
import datetime, time

_exchange_kraken = ccxt.kraken()


def _get_epoch_seconds_before(minutes_before):
    now_epoch_seconds = int(time.time() // 60) * 60
    return (now_epoch_seconds - minutes_before * 60)


def _get_epoch_millis_before(minutes_before):
    return _get_epoch_seconds_before(minutes_before) * 1000


def fetch_closes_since(exchange, symbols, since_epoch_seconds):
    now_minus_one_minute_epoch_millis = (int(time.time() // 60) - 1) * 60 * 1000
    since_epoch_milli = since_epoch_seconds * 1000
    epoch_millis = [since_epoch_milli]
    while epoch_millis[-1] < now_minus_one_minute_epoch_millis:
        epoch_millis.append(epoch_millis[-1] + 60 * 1000)
    time_and_values = [epoch_millis]
    columns = ['timestamp']

    for symbol in symbols:
        tohlcv = exchange.fetch_ohlcv(symbol, '1m', since_epoch_milli)

        ts = [bar[0] for bar in tohlcv]
        cs = [bar[4] for bar in tohlcv]
        while ts[0] > epoch_millis[0]:
            ts = [ts[0] - 60 * 1000] + ts
            cs = [cs[0]] + cs

        while ts[0] < epoch_millis[0]:
            ts = ts[1:]
            cs = cs[1:]

        while ts[-1] < epoch_millis[-1]:
            ts.append(ts[-1])
            cs.append(cs[-1])

        while ts[-1] > epoch_millis[-1]:
            ts = ts[:-1]
            cs = cs[:-1]
            
        time_and_values.append(cs)
        columns.append(symbol)

    df = pd.DataFrame(list(zip(*time_and_values)), columns =columns)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df = df.set_index('timestamp')
    return df


def fetch_closes(exchange, symbols, window_minutes):
    epoch_seconds_first = _get_epoch_seconds_before(window_minutes)
    return fetch_closes_since(exchange, symbols, epoch_seconds_first)


class PriceCache:
    def __init__(self, symbols, windows_minutes, df_prices=None):
        self.symbols = symbols
        self.windows_minutes = windows_minutes
        self.df_prices = df_prices

    def get_df_prices(self):
        if self.df_prices is None or len(self.df_prices) == 0:
            self.df_prices = fetch_closes(_exchange_kraken, self.symbols, self.windows_minutes)

        recent_epoch_seconds = self.df_prices.index[-1].timestamp()
        now_minus_one_minute_epoch_seconds = (int(time.time() // 60) - 1) * 60

        if recent_epoch_seconds < now_minus_one_minute_epoch_seconds:
            df_recent_prices = fetch_closes_since(_exchange_kraken, self.symbols, recent_epoch_seconds + 60)
            if len(df_recent_prices) > 0:
                self.df_prices = pd.concat([self.df_prices, df_recent_prices])

        return self.df_prices



