import datetime
import pandas as pd, numpy as np

# the eigen_vecs are for calculation only, the actual distribution over the asset is returned in `wgts`.
def calc_autocov_matrix(arr, lag):
    m = arr.shape[0]  # sample size.
    arr_lead = arr[lag:]
    arr_lag = arr[:m-lag]
    return 1.0 / (m - lag - 1) * (arr_lead - np.nanmean(arr_lead, axis=0)).T @ (arr_lag - np.nanmean(arr_lag, axis=0))

def align_eigs_transpose(mat, *df_values_T):
    var_eigen_vals, var_eigen_vecs = np.linalg.eig(mat)

    asc = np.argsort(var_eigen_vals)
    var_eigen_vals, var_eigen_vecs = var_eigen_vals[asc], var_eigen_vecs[:, asc]

    # allign the sign of the last element of the first eigen vector.
    if np.sign(var_eigen_vecs[-1, 0]) < 0:
        var_eigen_vecs = var_eigen_vecs * -1

    # allign the sign of the constructed portfolio
    if np.sign((np.transpose(df_values_T) @ var_eigen_vecs[:,0])[0]) < 0:
        var_eigen_vecs = var_eigen_vecs * -1

    #wgts_normalized = np.array([wgt / np.linalg.norm(wgt) for wgt in wgts])
    return var_eigen_vals, var_eigen_vecs
