import pandas as pd, numpy as np
import logging

import algo.minimal_predictability.calculate
import algo.statarbitrage.bband
import algo.trading.prices

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

    def get_default_param(symbols):
        return TradingParam(symbols, default_fitting_window_minutes, default_rebalance_period_minutes, default_bband_trading_param)


class Status:
    def __init__(self, weight):
        self.weight = weight

    def init_status(df_prices):
        _, var_eigen_vecs, wgts = algo.minimal_predictability.calculate.get_var1_wgts_values_transpose(*df_prices.values.T)
        status = Status(wgts[:,0])
        return status


class TradeManager:
    def __init__(self, symbols):
        self.price_cache = algo.trading.prices.PriceCache(symbols, default_max_window)
        self.status = Status.init_status(self.price_cache.get_df_prices())
        self.trading_param = TradingParam.get_default_param(symbols)
        self.last_rebalance_epoch_seconds = 0
        self.df_prices = self.price_cache.get_df_prices()
        self.on_price_update()


    def tick(self):
        print('tick')
        df_prices = self.price_cache.get_df_prices()
        price_updated = self.df_prices.index[-1].to_datetime64() == df_prices.index[-1].to_datetime64()
        self.df_prices = df_prices
        if price_updated:
            self.on_price_update()


    def on_price_update(self):
        '''
        decide if rebalance should happen
        if so rebalance and update weight in the status
        get if position changed
        if position changes, execute the position change.
        '''
        print('on_price_update')
        if self.get_if_rebalance():
            self.rebalance_weight()

        position_changed = self.get_position_changed()
        if position_changed:
            print('[on_price_update] position has changed')
            pass


    def get_if_rebalance(self):
        recent_epoch_seconds = self.df_prices.index[-1].to_datetime64().astype('int') // 10**9
        if recent_epoch_seconds < self.last_rebalance_epoch_seconds + self.trading_param.rebalance_period_minutes * 60:
            return False
        return True


    def rebalance_weight(self):
        print('rebalance_weight')
        _, var_eigen_vecs, wgts = algo.minimal_predictability.calculate.get_var1_wgts_values_transpose(*self.df_prices.values.T)
        self.status.weight = wgts[:,0]
        last_rebalance_epoch_seconds = self.df_prices.index[-1].to_datetime64().astype('int') // 10**9
        # gracefully anchor (5 minutes)
        last_rebalance_epoch_seconds = int(last_rebalance_epoch_seconds // 300) * 300
        self.last_rebalance_epoch_seconds = last_rebalance_epoch_seconds


    def get_position_changed(self):
        print('get_position_changed')
        df_features = algo.statarbitrage.bband.add_features(self.df_prices, self.status.weight, self.trading_param.bband_trading_param)
        return df_features.iloc[-1].position_changed

