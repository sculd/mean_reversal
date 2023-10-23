import pandas as pd, numpy as np

def bband(values, window, stds):
    df_band = values.copy()
    df_band['upper'] = (values.ewm(alpha=0.1).mean() + stds * values.rolling(window).std()) # * values.rolling(window).apply(adjustment_factor))
    df_band['lower'] = (values.ewm(alpha=0.1).mean() - stds * values.rolling(window).std()) # / values.rolling(window).apply(adjustment_factor))
    return df_band   

class BBandTradingParam:
    def __init__(self, bb_windows, bb_stdev):
        self.bb_windows, self.bb_stdev = bb_windows, bb_stdev

def add_features(df_prices, wgt, bband_trading_param):
    df_prices_weighted = df_prices * wgt
    values = df_prices_weighted.sum(axis=1).to_frame().rename(columns={0: 'value'})

    # rebalance
    if type(wgt) is np.ndarray or type(wgt) is pd.core.series.Series:
        values['rebalanced'] = 0
    else:
        values['rebalanced'] = wgt.sum(axis=1).to_frame().diff().fillna(0)
    values = values.fillna(0)
    values['rebalanced'] = values[['rebalanced']].where(values.rebalanced == 0, 1)

    rebalance_ages = [1]
    for i in range(1, len(values.index)):
        rebalance_age = rebalance_ages[-1] + 1
        if values.rebalanced.values[i] > 0:
            rebalance_age = 1
        rebalance_ages.append(rebalance_age)
    values['rebalance_age'] = rebalance_ages

    # bband and crossing
    '''
    upper, middle, lower = talib.BBANDS(values.value, bband_trading_param.bb_windows, bband_trading_param.bb_stdev, matype=MA_Type.T3)
    '''
    df_band = bband(values.value, bband_trading_param.bb_windows, bband_trading_param.bb_stdev)
    upper, lower = df_band.upper, df_band.lower

    values['upper'] = upper
    values['lower'] = lower 
    #upper, middle, lower, values = upper.dropna(), middle.dropna(), lower.dropna(), values.dropna()
    
    values['value_prev'] = values.value.shift()
    values['lower_crossed_upward'] = ((values.value_prev <= lower.shift()) & (values.value >= lower)).astype(np.int32)
    values['lower_crossed_downward'] = ((values.value_prev >= lower.shift()) & (values.value <= lower)).astype(np.int32)
    values['upper_crossed_upward'] = ((values.value_prev <= upper.shift()) & (values.value >= upper)).astype(np.int32)
    values['upper_crossed_downward'] = ((values.value_prev >= upper.shift()) & (values.value <= upper)).astype(np.int32)
    values = values.dropna()

    # in/out position with bband
    in_positions = [0]
    for i in range(1, len(values.index)):
        in_position = in_positions[-1]
        # if i_decision = i-1, the position of current is one step delayed to make it more realistic.
        # on the other hand, it might be too unrealistically strigent condition
        decision_delay = 0
        i_decision = i - decision_delay
        if values.lower_crossed_upward.values[i_decision] > 0: in_position = 1
        #elif values.upper_crossed_upward.values[i_decision] > 0: in_position = 1
        elif values.upper_crossed_downward.values[i_decision] > 0: in_position = 0
        #elif values.lower_crossed_downward.values[i_decision] > 0: in_position = 0
        in_positions.append(in_position)
    values['in_position'] = in_positions
    values['position_changed'] = values.in_position.diff()

    # position size
    df_prices_weighted['pos_mean_size'] = df_prices_weighted.where(df_prices_weighted > 0).mean(1)
    df_prices_weighted['neg_mean_size'] = -df_prices_weighted.where(df_prices_weighted <= 0).mean(1)
    df_prices_weighted['mean_size'] = df_prices_weighted[['pos_mean_size', 'neg_mean_size']].mean(1)
    values['mean_size'] = df_prices_weighted['mean_size']

    position_sizes = [0]
    for i in range(1, len(values.index)):
        position_size = position_sizes[-1]
        # the position of current is one step delayed to make it more realistic.
        if values.position_changed.values[i-1] > 0:
            position_size = values.mean_size.values[i]
        if values.position_changed.values[i-1] < 0:
            position_size = 0
        position_sizes.append(position_size)
    values['position_size'] = position_sizes

    # pnl
    # the result of position of current cycle gets reflected in the next cycle to prevent look-ahead bias.
    values['profit_raw'] = values.value.diff() * values.in_position.shift()
    values['profit'] = values.value.pct_change() * values.in_position.shift()

    return values

