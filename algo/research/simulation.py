import pandas as pd, numpy as np
import algo.statarbitrage.bband
import algo.minimal_predictability.calculate
from numpy_ext import rolling_apply as rolling_apply_ext


def get_var1_wgts_values_transpose_rolling(df_prices, window, rebalance_period_minutes, order, if_evecs):
    '''
    order: 0 for the smallest eigen value, -1 for the largest.
    if_evecs: True for eigen vectors, False for weights (e-vecs / sqrt(cov))
    '''
    i = 1 if if_evecs else 2
    rolling_wgt = rolling_apply_ext(lambda *vsT: algo.minimal_predictability.calculate.get_var1_wgts_values_transpose(*vsT)[i][:,order], window, *df_prices.values.T)
    df_rolling_wgt = pd.DataFrame(rolling_wgt, index=df_prices.index, columns=df_prices.columns)
    # shift by one time unit as the weight up to now will practically be applied in the next step. (?)
    df_rolling_wgt = df_rolling_wgt.shift()
    #df_rolling_wgt_resampled = df_rolling_wgt.resample(f'{rebalance_period_minutes}min').first().resample(f'{sample_unit_minutes}min').first().ffill()
    df_rolling_wgt_resampled = df_rolling_wgt.resample(f'{rebalance_period_minutes}min').first()
    return df_rolling_wgt, df_rolling_wgt_resampled

class StatArbitrageTradingParam:
    def __init__(self, train_data_sample_period_minutes, fitting_window, rebalance_period_minutes, bband_trading_param):
        self.train_data_sample_period_minutes = train_data_sample_period_minutes
        self.fitting_window = fitting_window
        self.rebalance_period_minutes = rebalance_period_minutes
        self.bband_trading_param = bband_trading_param

def get_trading_result(df_prices, symbols, stat_arbitrage_trading_param, if_evecs):
    df_prices_train_sampled = df_prices[symbols].resample(f'{stat_arbitrage_trading_param.train_data_sample_period_minutes}min').last().dropna()
    df_rolling_wgt, df_rolling_wgt_resampled = get_var1_wgts_values_transpose_rolling(
        df_prices_train_sampled, 
        window=stat_arbitrage_trading_param.fitting_window, 
        rebalance_period_minutes=stat_arbitrage_trading_param.rebalance_period_minutes, 
        order=0, if_evecs=if_evecs)

    df_prices_list = []
    head_buffer_length = stat_arbitrage_trading_param.bband_trading_param.bb_windows
    wgt_resammpled = df_rolling_wgt_resampled
    for i, index_head in enumerate(wgt_resammpled.index):
        if i == len(wgt_resammpled.index)-1: continue

        index_head_buffered = index_head - pd.Timedelta(minutes=head_buffer_length)
        index_tail = wgt_resammpled.index[i+1]
        df_prices_i = df_prices[(df_prices.index < index_tail) & (df_prices.index >= index_head_buffered)]
        df_prices_list.append((index_head_buffered, index_head, index_tail, df_prices_i, wgt_resammpled.loc[index_head]))

    values_list = []
    for index_head_buffered, index_head, index_tail, df_prices_i, wgt in df_prices_list:
        values_i = algo.statarbitrage.bband.add_features(df_prices_i, wgt, stat_arbitrage_trading_param.bband_trading_param, rebalance_buffer=head_buffer_length)
        if len(values_i) > 0:
            values_i['value_0'] = values_i.value - values_i.value.iloc[0]
            if values_i.at[values_i.index[-1], 'in_position'] == 1:
                values_i.at[values_i.index[-1], 'position_changed'] = -1
        values_list.append(values_i)

    return values_list


