import pandas as pd, numpy as np

def bband(values, window, stds):
    df_band = values.copy()
    df_band['upper'] = (values.ewm(alpha=0.1).mean() + stds * values.rolling(window).std()) # * values.rolling(window).apply(adjustment_factor))
    df_band['lower'] = (values.ewm(alpha=0.1).mean() - stds * values.rolling(window).std()) # / values.rolling(window).apply(adjustment_factor))
    return df_band   

class BBandTradingParam:
    def __init__(self, bb_windows, bb_stdev):
        self.bb_windows = bb_windows
        self.bb_stdev = bb_stdev


def add_features(df_prices_bband, df_prices_trading, wgt, bband_trading_param, rebalance_buffer=0):
    df_prices_bband_weighted = df_prices_bband * wgt
    values_bband = df_prices_bband_weighted.sum(axis=1).to_frame().rename(columns={0: 'value'})
    
    df_band = bband(values_bband.value, bband_trading_param.bb_windows, bband_trading_param.bb_stdev)
    upper, lower = df_band.upper, df_band.lower
    upper, lower = upper.resample('1min').ffill(), lower.resample('1min').ffill()
    
    df_prices_trading_weighted = df_prices_trading * wgt
    values_trading = df_prices_trading_weighted.sum(axis=1).to_frame().rename(columns={0: 'value'})
    
    # rebalance
    if type(wgt) is np.ndarray or type(wgt) is pd.core.series.Series:
        values_trading['rebalanced'] = 0
    else:
        values_trading['rebalanced'] = wgt.sum(axis=1).to_frame().diff().fillna(0)
    values_trading = values_trading.fillna(0)
    values_trading['rebalanced'] = values_trading[['rebalanced']].where(values_trading.rebalanced == 0, 1)
    
    rebalance_ages = [1 - rebalance_buffer]
    for i in range(1, len(values_trading.index)):
        rebalance_age = rebalance_ages[-1] + 1
        if values_trading.rebalanced.values[i] > 0:
            rebalance_age = 1
        rebalance_ages.append(rebalance_age)
    values_trading['rebalance_age'] = rebalance_ages
    
    values_trading['upper'] = upper
    values_trading['lower'] = lower 
    values_trading = values_trading.ffill()
    upper, lower = values_trading['upper'], values_trading['lower']
    #upper, middle, lower, values = upper.dropna(), middle.dropna(), lower.dropna(), values.dropna()

    values_trading['value_prev'] = values_trading.value.shift()
    values_trading['lower_crossed_upward'] = ((values_trading.value_prev <= lower.shift()) & (values_trading.value >= lower)).astype(np.int32)
    values_trading['lower_crossed_downward'] = ((values_trading.value_prev >= lower.shift()) & (values_trading.value <= lower)).astype(np.int32)
    values_trading['upper_crossed_upward'] = ((values_trading.value_prev <= upper.shift()) & (values_trading.value >= upper)).astype(np.int32)
    values_trading['upper_crossed_downward'] = ((values_trading.value_prev >= upper.shift()) & (values_trading.value <= upper)).astype(np.int32)
    values_trading = values_trading.dropna()
    
    
    # in/out position with bband
    in_positions = [0]
    for i in range(1, len(values_trading.index)):
        in_position = in_positions[-1]
        # if i_decision = i-1, the position of current is one step delayed to make it more realistic.
        # on the other hand, it might be too unrealistically strigent condition
        decision_delay = 0
        i_decision = i - decision_delay
        if values_trading.lower_crossed_upward.values[i_decision] > 0: in_position = 1
        #elif values_trading.upper_crossed_upward.values[i_decision] > 0: in_position = 1
        elif values_trading.upper_crossed_downward.values[i_decision] > 0: in_position = 0
        #elif values_trading.lower_crossed_downward.values[i_decision] > 0: in_position = 0
        in_positions.append(in_position)
    values_trading['in_position'] = in_positions
    values_trading['position_changed'] = values_trading.in_position.diff()
    
    # position size
    df_prices_trading_weighted['pos_mean_size'] = df_prices_trading_weighted.where(df_prices_trading_weighted > 0).mean(1)
    df_prices_trading_weighted['neg_mean_size'] = -df_prices_trading_weighted.where(df_prices_trading_weighted <= 0).mean(1)
    df_prices_trading_weighted['mean_size'] = df_prices_trading_weighted[['pos_mean_size', 'neg_mean_size']].mean(1)
    values_trading['mean_size'] = df_prices_trading_weighted['mean_size']
    
    position_sizes = [0]
    for i in range(1, len(values_trading.index)):
        position_size = position_sizes[-1]
        # the position of current is one step delayed to make it more realistic.
        if values_trading.position_changed.values[i-1] > 0:
            position_size = values_trading.mean_size.values[i]
        if values_trading.position_changed.values[i-1] < 0:
            position_size = 0
        position_sizes.append(position_size)
    values_trading['position_size'] = position_sizes
    
    # pnl
    # the result of position of current cycle gets reflected in the next cycle to prevent look-ahead bias.
    values_trading['profit_raw'] = values_trading.value.diff() * values_trading.in_position.shift()
    values_trading['profit'] = values_trading.value.pct_change() * values_trading.in_position.shift()
    values_trading = values_trading.dropna()

    return values_trading
