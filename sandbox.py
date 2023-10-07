import data.research
import minimal_predictability.calculate


df_prices = data.research.get_close_between_date(1, data.research.symbols, data.research.date_str_20220919, data.research.date_str_20220922)
var_eigen_vals, var_eigen_vecs, wgts = minimal_predictability.calculate.get_var1_wgts_values_transpose(*df_prices.values.T)

print('hello world')
print(var_eigen_vals)
print(var_eigen_vecs)
print(wgts)
