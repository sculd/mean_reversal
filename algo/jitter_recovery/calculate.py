import pandas as pd, numpy as np
from collections import defaultdict

defaultjump_window = 30

default_jump_threshold, default_drop_from_jump_threshold, default_exit_jumpt_threshold = 0.20, -0.05, 0.03

class JitterRecoveryFeatureParam:
    def __init__(self, jump_window):
        self.jump_window = jump_window

    def get_default_param():
        return JitterRecoveryFeatureParam(
            defaultjump_window)

class JitterRecoveryTradingParam:
    def __init__(self, jitter_recover_feature_param, jump_threshold, drop_from_jump_threshold, exit_jumpt_threshold):
        self.jitter_recover_feature_param = jitter_recover_feature_param
        self.jump_thresholdv= jump_threshold
        self.drop_from_jump_threshold = drop_from_jump_threshold
        self.exit_jumpt_threshold = exit_jumpt_threshold

    def get_default_param():
        return JitterRecoveryTradingParam(
            JitterRecoveryFeatureParam.get_default_param(), default_jump_threshold, default_drop_from_jump_threshold, default_exit_jumpt_threshold)
    

def get_changes_1dim(values):
    '''
    values is a 1 dimensional array.
    '''
    l = values.shape[0]
    if l < 1: return None

    if len(values.shape) == 2:
        values = [v[0] for v in values]

    ch_largest, ch_smallest = 0, 0
    distance_largest_ch, distance_smallest_ch = 1, 1
    ch_since_largest, ch_since_smallest = 0, 0

    first_v, last_v = values[0], values[-1]

    for i, v in enumerate(values):
        if first_v == 0:
            ch = 0
        else:
            ch = (v - first_v) / first_v
        if v == 0:
            ch_since = 0
        else:
            ch_since = (last_v - v) / v
        d =  l-1-i
        distance_largest_ch, ch_since_largest, ch_largest = (distance_largest_ch, ch_since_largest, ch_largest,) if ch_largest > ch else (d, ch_since, ch,)
        distance_smallest_ch, ch_since_smallest, ch_smallest = (distance_smallest_ch, ch_since_smallest, ch_smallest,) if ch_smallest < ch else (d, ch_since, ch,)

    return {
        'value': values[-1],
        'ch_largest': ch_largest, 'ch_smallest': ch_smallest,
        'ch_since_largest': ch_since_largest, 'ch_since_smallest': ch_since_smallest,
        'distance_largest_ch': distance_largest_ch, 'distance_smallest_ch': distance_smallest_ch,
        }


def get_feature_df(dfs, jitter_recover_feature_param):
    window = jitter_recover_feature_param.jump_window
    return pd.DataFrame([get_changes_1dim(df_.values) for df_ in dfs[['close']].rolling(window, min_periods=window)], index=dfs.index)

def add_trading_columns(df_feature, jitter_recover_trading_param):
    in_positions = [0]
    lowest_since_enters = [0]
    timedelta_since_position_enters = [0]
    value_at_enters = [0]
    ch_from_enters = [0]
    ch_from_lowest_since_enters = [0]
    for i in range(1, len(df_feature.index)):
        in_position = in_positions[-1]
        value_at_enter = value_at_enters[-1]
        lowest_since_enter = lowest_since_enters[-1]
        timedelta_since_position_enter = timedelta_since_position_enters[-1]
        ch_from_enter = ch_from_enters[-1]
        ch_from_lowest_since_enter = ch_from_lowest_since_enters[-1]
        # if i_decision = i-1, the position of current is one step delayed to make it more realistic.
        # on the other hand, it might be too unrealistically strigent condition
        decision_delay = 0
        i_decision = i - decision_delay
        v = df_feature.value.values[i]
        if in_position == 1:
            if v < lowest_since_enter:
                lowest_since_enter = v
            timedelta_since_position_enter = timedelta_since_position_enters[-1] + 1
            ch_from_enter =  (v - value_at_enter) / value_at_enter
            ch_from_lowest_since_enter = (v - lowest_since_enter) / lowest_since_enter
    
            if ch_from_lowest_since_enter > jitter_recover_trading_param.exit_jumpt_threshold:
                in_position = 0
        elif df_feature.ch_largest.values[i_decision] > jitter_recover_trading_param.jump_thresholdv \
            and df_feature.ch_since_largest.values[i_decision] < jitter_recover_trading_param.drop_from_jump_threshold: 
            in_position = 1
            value_at_enter = v
            lowest_since_enter = df_feature.value.values[i]
            timedelta_since_position_enter = 0
            ch_from_enter = 0
            ch_from_lowest_since_enter = 0
        else:
            in_position = 0
            value_at_enter = 0
            lowest_since_enter = 0
            timedelta_since_position_enter = 0
            ch_from_enter = 0
            ch_from_lowest_since_enter = 0
    
        in_positions.append(in_position)
        value_at_enters.append(value_at_enter)
        lowest_since_enters.append(lowest_since_enter)
        timedelta_since_position_enters.append(timedelta_since_position_enter)
        ch_from_enters.append(ch_from_enter)
        ch_from_lowest_since_enters.append(ch_from_lowest_since_enter)
    
    df_feature['in_position'] = in_positions
    df_feature['value_at_enter'] = value_at_enters
    df_feature['position_changed'] = df_feature.in_position.diff()
    df_feature['lowest_since_enter'] = lowest_since_enters
    df_feature['timedelta_since_position_enter'] = timedelta_since_position_enters
    df_feature['ch_from_enter'] = ch_from_enters
    df_feature['ch_from_lowest_since_enter'] = ch_from_lowest_since_enters
    df_feature['profit_raw'] = -df_feature.value.diff() * df_feature.in_position.shift()
    df_feature['profit'] = -df_feature.value.pct_change() * df_feature.in_position.shift()

    return df_feature
