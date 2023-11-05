import pandas as pd, numpy as np, datetime
import logging

import algo.minimal_predictability.calculate
import algo.statarbitrage.bband
import algo.trading.prices
import algo.trading.execution

default_fitting_window = 180
default_train_data_sample_period_minutes = 10
default_rebalance_period_minutes = 120
default_bband_window = 60
default_bband_trading_param = algo.statarbitrage.bband.BBandTradingParam(default_bband_window, 2.0)
default_if_evecs = True


class TradingParam:
    def __init__(self, fitting_window, default_train_data_sample_period_minutes, rebalance_period_minutes, if_evecs, bband_trading_param):
        '''
        if_evecs: True for eigen vectors, False for weights (e-vecs / sqrt(cov))
        '''
        self.fitting_window = fitting_window
        self.train_data_sample_period_minutes = default_train_data_sample_period_minutes
        self.rebalance_period_minutes = rebalance_period_minutes
        self.if_evecs = if_evecs
        self.bband_trading_param = bband_trading_param

    def get_max_window_minutes(self):
        return max(self.fitting_window * self.train_data_sample_period_minutes, self.rebalance_period_minutes, self.bband_trading_param.bb_windows)

    def get_default_param():
        return TradingParam(default_fitting_window, default_train_data_sample_period_minutes, default_rebalance_period_minutes, default_if_evecs, default_bband_trading_param)


class Status:
    def __init__(self, weight):
        self.weight = weight
        self.last_rebalance_epoch_seconds = 0

    def get_weight(df_prices, if_evecs):
        '''
        if_evecs: True for eigen vectors, False for weights (e-vecs / sqrt(cov))
        '''   
        # ValueError
        try:
            _, var_eigen_vecs, wgts = algo.minimal_predictability.calculate.get_var1_wgts_values_transpose(*df_prices.values.T)
            return var_eigen_vecs[:,0] if if_evecs else wgts[:,0]
        except ValueError as ex:
            logging.warn(ex)
            return np.array([0] * len(df_prices.columns))

    def init_status(df_prices_train, if_evecs):
        wg = Status.get_weight(df_prices_train, if_evecs)
        status = Status(wg)
        return status

    def get_if_rebalance(self, updated_epoch_seconds, rebalance_period_minutes):
        next_rebalance_epoch_seconds = self.last_rebalance_epoch_seconds + rebalance_period_minutes * 60;
        next_rebalance_epoch_seconds = int(next_rebalance_epoch_seconds // (rebalance_period_minutes * 60)) * (rebalance_period_minutes * 60)
        if updated_epoch_seconds < next_rebalance_epoch_seconds:
            return False
        return True

    def rebalance_weight(self, df_prices, df_prices_train, if_evecs):
        wg = Status.get_weight(df_prices_train, if_evecs)
        self.weight = wg
        logging.info(f'rebalance at {df_prices.iloc[-1].name} ({int(df_prices.iloc[-1].name.timestamp())}), obtained weight: {self.weight}')
        last_rebalance_epoch_seconds = df_prices.index[-1].to_datetime64().astype('int') // 10**9
        current_rebalance_epoch_seconds = last_rebalance_epoch_seconds + 60
        # gracefully anchor (5 minutes)
        current_rebalance_epoch_seconds = int(current_rebalance_epoch_seconds // 300) * 300
        self.last_rebalance_epoch_seconds = current_rebalance_epoch_seconds


class TradeManager:
    def __init__(self, symbols, trading_param=None, price_cache=None):
        self.trading_param = trading_param if trading_param is not None else TradingParam.get_default_param()
        self.price_cache = price_cache if price_cache is not None else algo.trading.prices.PriceCache(symbols, self.trading_param.get_max_window_minutes())
        self.df_prices = self.price_cache.get_df_prices()
        self.status = Status.init_status(self.get_trading_df_price(), self.trading_param.if_evecs)
        self.trade_execution = algo.trading.execution.TradeExecution(symbols)
        self.on_price_update()


    def tick(self):
        logging.debug('tick')
        df_prices = self.price_cache.get_df_prices()
        price_updated = self.df_prices.index[-1].to_datetime64() != df_prices.index[-1].to_datetime64()
        if price_updated:
            logging.debug(f'price is updated, previous t: {self.df_prices.index[-1].to_datetime64()}, updated t: {df_prices.index[-1].to_datetime64()}')
            self.df_prices = df_prices
            self.on_price_update()


    def on_price_update(self):
        '''
        decide if rebalance should happen
        if so rebalance and update weight in the status
        get if position changed
        if position changes, execute the position change.
        '''
        logging.debug(f'on_price_update:\n{self.df_prices.iloc[-1]}')

        if self.get_if_rebalance():
            last_epoch_seconds = int(self.df_prices.iloc[-1].name.timestamp())

            if self.trade_execution.direction == 1:
                logging.info(f'[on_price_update] should rebalance at {self.df_prices.iloc[-1].name}({last_epoch_seconds}), exiting the current position')
                self.trade_execution.get_out_of_current_position(last_epoch_seconds, self.df_prices.iloc[-1], self.status.weight)
            self.rebalance_weight()

        position_changed = self.get_position_changed()
        if position_changed != 0:
            if position_changed == 1:
                position_changed += 0
            last_epoch_seconds = self.df_prices.index[-1].to_datetime64().astype('int') // 10**9
            logging.info(f'[on_price_update] at {self.df_prices.iloc[-1].name}({last_epoch_seconds}), position has changed: {position_changed}')
            self.trade_execution.execute(int(self.df_prices.iloc[-1].name.timestamp()), self.df_prices.iloc[-1], self.status.weight, position_changed)

    def get_current_epoch_seconds(self):
        last_epoch_seconds = self.df_prices.index[-1].to_datetime64().astype('int') // 10**9
        current_epoch_seconds = last_epoch_seconds + 60
        return current_epoch_seconds

    def get_trading_df_price(self):
        df_prices_resampled = self.df_prices.resample(f'{self.trading_param.train_data_sample_period_minutes}min').last().dropna()
        l = len(df_prices_resampled)
        df_prices_train = df_prices_resampled.iloc[max(0, l-self.trading_param.fitting_window):]
        return df_prices_train

    def get_if_rebalance(self):
        current_epoch_seconds = self.get_current_epoch_seconds()
        return self.status.get_if_rebalance(current_epoch_seconds, self.trading_param.rebalance_period_minutes)

    def rebalance_weight(self):
        self.status.rebalance_weight(self.df_prices, self.get_trading_df_price(), self.trading_param.if_evecs)
        logging.info(f'rebalanced at {self.df_prices.index[-1].to_datetime64()} weight: {self.status.weight}')

    def get_position_changed(self):
        logging.debug('get_position_changed')
        df = self.df_prices
        dt_head = df.index[-1].floor(f'{self.trading_param.rebalance_period_minutes}min').to_pydatetime() - datetime.timedelta(minutes=self.trading_param.bband_trading_param.bb_windows)
        df = df[df.index >= dt_head]
        df_features = algo.statarbitrage.bband.add_features(df, self.status.weight, self.trading_param.bband_trading_param)
        if len(df_features) == 0:
            return False
        return df_features.iloc[-1].position_changed

