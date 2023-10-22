import datetime
import pandas as pd, numpy as np
import scipy
from numpy_ext import rolling_apply as rolling_apply_ext

# the eigen_vecs are for calculation only, the actual distribution over the asset is returned in `wgts`.
def calc_autocov_matrix(arr, lag):
    m = arr.shape[0]  # sample size.
    arr_lead = arr[lag:]
    arr_lag = arr[:m-lag]
    return 1.0 / (m - lag - 1) * (arr_lead - np.nanmean(arr_lead, axis=0)).T @ (arr_lag - np.nanmean(arr_lag, axis=0))

def get_var1_wgts_values_transpose(*df_values_T):
    cov = np.cov(df_values_T)
    cov_inv = np.linalg.inv(cov)
    cov_inv_sqrt = scipy.linalg.sqrtm(cov_inv)
    autocov = calc_autocov_matrix(np.transpose(df_values_T), 1)
    #var_predictability = cov_inv_sqrt @ autocov @ cov @ autocov.T @ cov_inv_sqrt.T
    var_predictability = cov_inv_sqrt @ autocov @ cov_inv @ autocov.T @ cov_inv_sqrt.T

    var_eigen_vals, var_eigen_vecs = np.linalg.eig(var_predictability)

    asc = np.argsort(var_eigen_vals)
    var_eigen_vals, var_eigen_vecs = var_eigen_vals[asc], var_eigen_vecs[:, asc]
    wgts = cov_inv_sqrt @ var_eigen_vecs
    if np.sign((np.transpose(df_values_T) @ wgts[:,0])[-1]) < 0:
        pass # var_eigen_vecs, wgts = var_eigen_vecs * -1, wgts * -1

    # allign the sign of the last element of the first eigen vector.
    if np.sign(wgts[-1,0]) < 0:
        var_eigen_vecs, wgts = var_eigen_vecs * -1, wgts * -1

    # allign the sign of the constructed portfolio
    if np.sign((np.transpose(df_values_T) @ wgts[:,0])[0]) < 0:
        var_eigen_vecs, wgts = var_eigen_vecs * -1, wgts * -1

    #wgts_normalized = np.array([wgt / np.linalg.norm(wgt) for wgt in wgts])
    return var_eigen_vals, var_eigen_vecs, wgts

# The same function that takes df.values.T instead of df itself. This is used in the custom rolling function as the custom rolling function does not take the data frame but the arrays.
def get_var1_wgts(df):
    return get_var1_wgts_values_transpose(*df.values.T)


def get_var1_wgts_values_transpose_rolling(df_prices, window, rebalance_period, sample_unit_minutes, order, if_evecs):
    '''
    order: 0 for the smallest eigen value, -1 for the largest.
    if_evecs: True for eigen vectors, False for weights (e-vecs / sqrt(cov))
    '''
    i = 1 if if_evecs else 2
    rolling_wgt = rolling_apply_ext(lambda *vsT: get_var1_wgts_values_transpose(*vsT)[i][:,order], window, *df_prices.values.T)
    df_rolling_wgt = pd.DataFrame(rolling_wgt, index=df_prices.index, columns=df_prices.columns)
    # shift by one time unit as the weight up to now will practically be applied in the next step. (?)
    df_rolling_wgt = df_rolling_wgt.shift()
    #df_rolling_wgt_resampled = df_rolling_wgt.resample(f'{rebalance_period * sample_unit_minutes}min').first().resample(f'{sample_unit_minutes}min').first().ffill()
    df_rolling_wgt_resampled = df_rolling_wgt.resample(f'{rebalance_period * sample_unit_minutes}min').first()
    return df_rolling_wgt, df_rolling_wgt_resampled