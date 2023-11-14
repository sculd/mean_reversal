import pandas as pd, numpy as np
from enum import Enum
import algo.statarbitrage.bband
import algo.mean_reversion.predictability
import algo.mean_reversion.crossing_stat

from numpy_ext import rolling_apply as rolling_apply_ext

class MeanReversioMode(Enum):
    MINIMAL_PREDICTABILITY = 1
    CROSSING_STAT = 2


def get_minimal_predictability_wgts_values_transpose_rolling(df_prices, window, rebalance_period_minutes, order):
    '''
    order: 0 for the smallest eigen value, -1 for the largest.
    if_evecs: True for eigen vectors, False for weights (e-vecs / sqrt(cov))
    '''
    i = 1
    rolling_wgt = rolling_apply_ext(lambda *vsT: algo.mean_reversion.predictability.get_wgts_predictability_transpose(*vsT)[i][:,order], window, *df_prices.values.T)
    df_rolling_wgt = pd.DataFrame(rolling_wgt, index=df_prices.index, columns=df_prices.columns)
    # shift by one time unit as the weight up to now will practically be applied in the next step. (?)
    df_rolling_wgt = df_rolling_wgt.shift()
    #df_rolling_wgt_resampled = df_rolling_wgt.resample(f'{rebalance_period_minutes}min').first().resample(f'{sample_unit_minutes}min').first().ffill()
    df_rolling_wgt_resampled = df_rolling_wgt.resample(f'{rebalance_period_minutes}min').first()
    return df_rolling_wgt, df_rolling_wgt_resampled

def get_crossing_stat_wgts_values_transpose_rolling(df_prices, window, rebalance_period_minutes, order):
    '''
    order: 0 for the smallest eigen value, -1 for the largest.
    if_evecs: True for eigen vectors, False for weights (e-vecs / sqrt(cov))
    '''
    i = 1
    rolling_wgt = rolling_apply_ext(lambda *vsT: algo.mean_reversion.crossing_stat.get_wgts_crossing_stat_transpose(*vsT)[i][:,order], window, *df_prices.values.T)
    df_rolling_wgt = pd.DataFrame(rolling_wgt, index=df_prices.index, columns=df_prices.columns)
    # shift by one time unit as the weight up to now will practically be applied in the next step. (?)
    df_rolling_wgt = df_rolling_wgt.shift()
    #df_rolling_wgt_resampled = df_rolling_wgt.resample(f'{rebalance_period_minutes}min').first().resample(f'{sample_unit_minutes}min').first().ffill()
    df_rolling_wgt_resampled = df_rolling_wgt.resample(f'{rebalance_period_minutes}min').first()
    return df_rolling_wgt, df_rolling_wgt_resampled


def get_trading_result(df_prices, symbols, stat_arbitrage_trading_param, mean_reversion_mode):
    df_prices_train_sampled = df_prices[symbols].resample(f'{stat_arbitrage_trading_param.train_data_sample_period_minutes}min').last().dropna()
    wgts_getter = None
    if mean_reversion_mode == MeanReversioMode.MINIMAL_PREDICTABILITY:
        wgts_getter = get_minimal_predictability_wgts_values_transpose_rolling
    elif mean_reversion_mode == MeanReversioMode.CROSSING_STAT:
        wgts_getter = get_crossing_stat_wgts_values_transpose_rolling

    df_rolling_wgt, df_rolling_wgt_resampled = wgts_getter(
        df_prices_train_sampled, 
        window=stat_arbitrage_trading_param.fitting_window, 
        rebalance_period_minutes=stat_arbitrage_trading_param.rebalance_period_minutes, 
        order=0)

    df_prices_bband = df_prices[symbols].resample(f'{stat_arbitrage_trading_param.bband_sample_period_minutes}min').last().dropna()
    df_prices_trading = df_prices[symbols].resample(f'{stat_arbitrage_trading_param.trading_sample_period_minutes}min').last().dropna()

    df_prices_list = []
    head_buffer_length = stat_arbitrage_trading_param.bband_trading_param.bb_windows
    wgt_resammpled = df_rolling_wgt_resampled.dropna()
    for i, index_head in enumerate(wgt_resammpled.index):
        if i == len(wgt_resammpled.index)-1: continue

        #index_head_buffered = index_head - pd.Timedelta(minutes=head_buffer_length)
        index_head_buffered = index_head - pd.Timedelta(minutes=head_buffer_length * stat_arbitrage_trading_param.bband_sample_period_minutes)
        index_tail = wgt_resammpled.index[i+1]
        df_prices_i_bband = df_prices_bband[(df_prices_bband.index < index_tail) & (df_prices_bband.index >= index_head_buffered)]
        df_prices_i_trading = df_prices_trading[(df_prices_trading.index < index_tail) & (df_prices_trading.index >= index_head)]
        df_prices_list.append((index_head_buffered, index_head, index_tail, df_prices_i_bband, df_prices_i_trading, wgt_resammpled.loc[index_head]))

    values_list = []
    for index_head_buffered, index_head, index_tail, df_prices_i_bband, df_prices_i_trading, wgt in df_prices_list:
        values_i = algo.statarbitrage.bband.add_features(df_prices_i_bband, df_prices_i_trading, wgt, stat_arbitrage_trading_param.bband_trading_param, rebalance_buffer=head_buffer_length)
        if len(values_i) > 0:
            values_i['value_0'] = values_i.value - values_i.value.iloc[0]
            if values_i.at[values_i.index[-1], 'in_position'] == 1:
                values_i.at[values_i.index[-1], 'position_changed'] = -1
        else:
            print(f'values_i is empty for df_prices_i_bband: {len(df_prices_i_bband)}, df_prices_i_trading: {len(df_prices_i_trading)}')
        values_list.append(values_i)

    return values_list

