import pandas as pd, numpy as np
import logging

import algo.minimal_predictability.calculate
import algo.statarbitrage.bband
import algo.trading.prices
import algo.trading.execution

default_fitting_window_minutes = 180
default_rebalance_period_minutes = 180
default_bband_window_minutes = 60
default_max_window = max(default_fitting_window_minutes, default_rebalance_period_minutes, default_bband_window_minutes)
default_bband_trading_param = algo.statarbitrage.bband.TradingParam(default_bband_window_minutes, 2.0)


class TradingParam:
    def __init__(self, symbols, fitting_window_minutes, rebalance_period_minutes, bband_trading_param):
        self.symbols = symbols
        self.fitting_window_minutes, self.rebalance_period_minutes = fitting_window_minutes, rebalance_period_minutes
        self.bband_trading_param = bband_trading_param

    def get_max_window_minutes(self):
        return max(self.fitting_window_minutes, self.rebalance_period_minutes, self.bband_trading_param.bb_windows)

    def get_default_param(symbols):
        return TradingParam(symbols, default_fitting_window_minutes, default_rebalance_period_minutes, default_bband_trading_param)


class Status:
    def __init__(self, weight):
        self.weight = weight
        self.last_rebalance_epoch_seconds = 0

    def init_status(df_prices):
        _, var_eigen_vecs, wgts = algo.minimal_predictability.calculate.get_var1_wgts_values_transpose(*df_prices.values.T)
        status = Status(wgts[:,0])
        return status

    def get_if_rebalance(self, updated_epoch_seconds, rebalance_period_minutes):
        if updated_epoch_seconds < self.last_rebalance_epoch_seconds + rebalance_period_minutes * 60:
            return False
        return True

    def rebalance_weight(self, df_prices):
        _, var_eigen_vecs, wgts = algo.minimal_predictability.calculate.get_var1_wgts_values_transpose(*df_prices.values.T)
        self.weight = wgts[:,0]
        last_rebalance_epoch_seconds = df_prices.index[-1].to_datetime64().astype('int') // 10**9
        # gracefully anchor (5 minutes)
        last_rebalance_epoch_seconds = int(last_rebalance_epoch_seconds // 300) * 300
        self.last_rebalance_epoch_seconds = last_rebalance_epoch_seconds


class TradeManager:
    def __init__(self, symbols, trading_param=None):
        self.trading_param = trading_param if trading_param is not None else TradingParam.get_default_param(symbols)
        self.price_cache = algo.trading.prices.PriceCache(symbols, self.trading_param.get_max_window_minutes())
        self.status = Status.init_status(self.price_cache.get_df_prices())
        self.trade_execution = algo.trading.execution.TradeExecution(symbols)
        self.df_prices = self.price_cache.get_df_prices()
        self.on_price_update()


    def tick(self):
        logging.debug('tick')
        df_prices = self.price_cache.get_df_prices()
        price_updated = self.df_prices.index[-1].to_datetime64() != df_prices.index[-1].to_datetime64()
        if price_updated:
            logging.info(f'price is updated, previous t: {self.df_prices.index[-1].to_datetime64()}, updated t: {df_prices.index[-1].to_datetime64()}')
            self.df_prices = df_prices
            self.on_price_update()


    def on_price_update(self):
        '''
        decide if rebalance should happen
        if so rebalance and update weight in the status
        get if position changed
        if position changes, execute the position change.
        '''
        logging.info(f'on_price_update:\n{self.df_prices.iloc[-1]}')
        if self.get_if_rebalance():
            self.rebalance_weight()

        position_changed = self.get_position_changed()
        if position_changed != 0:
            logging.info('[on_price_update] position has changed: {position_changed}')
            self.trade_execution(self.status.weight, position_changed)


    def get_if_rebalance(self):
        recent_epoch_seconds = self.df_prices.index[-1].to_datetime64().astype('int') // 10**9
        return self.status.get_if_rebalance(recent_epoch_seconds, self.trading_param.rebalance_period_minutes)


    def rebalance_weight(self):
        self.status.rebalance_weight(self.df_prices)
        logging.debug('rebalanced weight: {self.status.weight}')


    def get_position_changed(self):
        logging.debug('get_position_changed')
        df_features = algo.statarbitrage.bband.add_features(self.df_prices, self.status.weight, self.trading_param.bband_trading_param)
        return df_features.iloc[-1].position_changed

