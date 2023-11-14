import datetime
import pandas as pd, numpy as np
import scipy
from numpy_ext import rolling_apply as rolling_apply_ext
import algo.mean_reversion.common

def get_mat_crossing_stat_transpose(*df_values_T):
    cov = np.cov(df_values_T)
    cov_inv = np.linalg.inv(cov)
    cov_inv_sqrt = scipy.linalg.sqrtm(cov_inv)
    autocov = algo.mean_reversion.common.calc_autocov_matrix(np.transpose(df_values_T), 1)
    mat_predictability = cov_inv_sqrt @ autocov.T @ cov_inv_sqrt.T

    return mat_predictability
    
def get_crossing_stat_transpose(w, *df_values_T):
    mat_crossing_stat = get_mat_crossing_stat_transpose(*df_values_T)

    return w.T @ mat_crossing_stat @ w

def get_wgts_crossing_stat_transpose(*df_values_T):
    mat = get_mat_crossing_stat_transpose(*df_values_T)

    return algo.mean_reversion.common.align_eigs_transpose(mat, *df_values_T)


def get_wgts_crossing_stat(df):
    return get_wgts_crossing_stat_transpose(*df.values.T)


