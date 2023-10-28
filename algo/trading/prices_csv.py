import pandas as pd, numpy as np
import datetime, time
import logging

class BacktestCsvPriceCache:
    def __init__(self, csv_filename, symbols, windows_minutes):
        df_prices_history = pd.read_csv(csv_filename)
        df_prices_history['time'] = pd.to_datetime(df_prices_history['timestamp'], unit='s')
        if 'symbol' in df_prices_history.columns:
            df_prices_history_close = df_prices_history.pivot(index='time', columns='symbol', values='close')
        else:
            df_prices_history_close = df_prices_history.set_index('time')
        df_prices_history_close = df_prices_history_close[symbols]
        self.df_prices_history_close = df_prices_history_close

        self.symbols = symbols
        self.windows_minutes = windows_minutes
        self.df_prices_history = df_prices_history
        self.df_prices = None

        self.price_history_indices = df_prices_history_close.index.values
        self.price_history_epoch_seconds = [int(pd.to_datetime(t).timestamp()) for t in df_prices_history_close.index.values]
        self.price_history_values = df_prices_history_close.values
        self.history_read_i = 0
        self.latest_now_epoch_seconds = 0
        logging.info(f'csv price cache loaded {csv_filename}')
        

    def get_now_epoch_seconds(self, anchored):
        if self.history_read_i >= len(self.price_history_epoch_seconds):
            self.latest_now_epoch_seconds += 60
            return self.latest_now_epoch_seconds
    
        epoch_seconds = self.price_history_epoch_seconds[self.history_read_i] + 60 + 1
        if anchored:
            epoch_seconds = int(epoch_seconds//60) * 60
        self.latest_now_epoch_seconds = epoch_seconds
        return epoch_seconds

    def get_anchored_epoch_seconds_minutes_before(self, minutes_before):
        return (int(self.get_now_epoch_seconds(anchored=True) // 60) - minutes_before) * 60

    def get_anchored_one_minute_epoch_seconds(self):
        return self.get_anchored_epoch_seconds_minutes_before(1)

    def fetch_closes_since(self, since_epoch_seconds):
        columns = ['timestamp'] + self.symbols
        now_epoch_seconds = self.get_now_epoch_seconds(anchored=True)
        anchored_one_minute_epoch_seconds = self.get_anchored_one_minute_epoch_seconds()

        time_and_values_backward = []
        i = self.history_read_i - 1
        while i >= 0:
            epoch_seconds, values = self.price_history_epoch_seconds[i], self.price_history_values[i]
            if epoch_seconds < since_epoch_seconds:
                break
            if epoch_seconds < now_epoch_seconds:
                time_and_values_backward.append([epoch_seconds * 1000] + list(values))
            i -= 1

        time_and_values_forward = []
        while self.history_read_i < len(self.price_history_epoch_seconds):
            epoch_seconds, values = self.price_history_epoch_seconds[self.history_read_i], self.price_history_values[self.history_read_i]
            if epoch_seconds > anchored_one_minute_epoch_seconds:
                break
            time_and_values_forward.append([epoch_seconds * 1000] + list(values))
            self.history_read_i += 1

        time_and_values = time_and_values_backward[::-1] + time_and_values_forward

        df = pd.DataFrame(time_and_values, columns = columns)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        df = df.set_index('timestamp')
        return df

    def fetch_closes(self, window_minutes):
        epoch_seconds_first = self.get_anchored_epoch_seconds_minutes_before(window_minutes)
        return self.fetch_closes_since(epoch_seconds_first)

    def if_history_all_read(self):
        return self.history_read_i >= len(self.price_history_epoch_seconds)

    def warmup(self):
        while self.df_prices is None or (len(self.df_prices) < self.windows_minutes and not self.if_history_all_read()):
            self.get_df_prices()

    def get_df_prices(self):
        if self.df_prices is None:
            self.df_prices = self.fetch_closes(self.windows_minutes)

        if len(self.df_prices) == 0:
            return self.df_prices

        recent_epoch_seconds = self.df_prices.index[-1].timestamp()
        now_minus_one_minute_epoch_seconds = self.get_anchored_one_minute_epoch_seconds()
        now_minus_windows_minute_epoch_seconds = self.get_anchored_epoch_seconds_minutes_before(self.windows_minutes)

        if recent_epoch_seconds < now_minus_one_minute_epoch_seconds:
            df_recent_prices = self.fetch_closes_since(recent_epoch_seconds + 60)
            if len(df_recent_prices) > 0:
                self.df_prices = pd.concat([self.df_prices, df_recent_prices])

        #self.df_prices = self.df_prices[max(0, len(self.df_prices) - self.windows_minutes):]
        self.df_prices = self.df_prices[self.df_prices.index.astype(np.int64) // 10**9 >= now_minus_windows_minute_epoch_seconds]
        
        return self.df_prices


