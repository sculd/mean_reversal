import datetime
import pandas as pd, numpy as np
import scipy
import algo.mean_reversion.optimization
import algo.mean_reversion.crossing_stat
import algo.mean_reversion.predictability

def get_mat_combination_stat_transpose(*df_values_T, w_min_predictability=1, w_crossing_stat=1):
    mat_crossing_stat = algo.mean_reversion.crossing_stat.get_mat_crossing_stat_transpose(*df_values_T)
    mat_predictability = algo.mean_reversion.predictability.get_mat_predictability_transpose(*df_values_T)

    min_val_crossing_stat, _ = algo.mean_reversion.crossing_stat.get_wgts_crossing_stat_transpose(*df_values_T)
    min_val_predictability, _ = algo.mean_reversion.predictability.get_wgts_predictability_transpose(*df_values_T)

    mat = w_min_predictability * mat_crossing_stat / min_val_crossing_stat + w_crossing_stat * mat_predictability / min_val_predictability

    return mat 

def get_wgts_combination_stat_transpose(*df_values_T, w_min_predictability=1, w_crossing_stat=1):
    mat = get_mat_combination_stat_transpose(*df_values_T, w_min_predictability=w_min_predictability, w_crossing_stat=w_crossing_stat)

    min_vec, min_val = algo.mean_reversion.optimization.solve_min_quadratic(mat)
    # mat is not symmetric so the eigen decomposition can not be used here as it has imaginary e-vecs.
    return np.array([min_val]), np.array([min_vec]).T

def get_wgts(df):
    return get_wgts_combination_stat_transpose(*df.values.T)
