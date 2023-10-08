import pandas as pd, numpy as np

import minimal_predictability.calculate
import trading.bband
import trading_live.prices

default_fitting_window = 180
default_rebalance_period = 180
default_bband_window = 60
default_max_window = max(default_fitting_window, default_rebalance_period, default_bband_window)
default_bband_trading_param = trading.bband.TradingParam(default_bband_window, 2.0)


class TradingParam:
    def __init__(self, symbols, fitting_window, rebalance_period, bband_trading_param):
        self.symbols = symbols
        self.fitting_window, self.rebalance_period = fitting_window, rebalance_period
        self.bband_trading_param = bband_trading_param


class Status:
    def __init__(self, weight):
        self.weight = weight

    def init_status():
        df_prices = trading_live.prices.get_df_prices()
        _, var_eigen_vecs, wgts = minimal_predictability.calculate.get_var1_wgts_values_transpose(*df_prices.values.T)
        status = Status(wgts[:0])
        return status


class Trading:
    def __init__(self, symbols):
        self.status = Status.init_status()
        self.price_cache = trading_live.prices.PriceCache(symbols, default_max_window)
        self.bband_trading_param = default_bband_trading_param
        self.last_rebalance_epoch_seconds = 0


    def on_price_update(self):
        '''
        decide if rebalance should happen
        if so rebalance and update weight in the status
        get if position changed
        if position changes, execute the position change.
        '''
        if self.get_if_rebalance():
            self.rebalance_weight()

        position_changed = self.get_position_changed()
        if position_changed:
            pass



    def get_if_rebalance(self):
        return False


    def rebalance_weight(self):
        df_prices = self.price_cache.get_df_prices()
        _, var_eigen_vecs, wgts = minimal_predictability.calculate.get_var1_wgts_values_transpose(*df_prices.values.T)
        self.status.weight = wgts[:0]


    def get_position_changed(self):
        df_prices = self.price_cache.get_df_prices()
        df_features = trading.bband.add_features(df_prices, self.status.weight, self.bband_trading_param)
        return df_features.iloc[-1].position_changed


