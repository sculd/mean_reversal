import datetime
import pandas as pd, numpy as np

date_str_20220901 = "2022-09-01"
date_str_20220919 = "2022-09-19"
date_str_20220920 = "2022-09-20"
date_str_20220921 = "2022-09-21"
date_str_20220922 = "2022-09-22"
date_str_20220923 = "2022-09-23"
date_str_20220924 = "2022-09-24"
date_str_20220925 = "2022-09-25"
date_str_20220930 = "2022-09-30"

date_str_20230801 = "2023-08-01"
date_str_20230803 = "2023-08-03"
date_str_20230806 = "2023-08-06"
date_str_20230809 = "2023-08-09"
date_str_20230831 = "2023-08-31"

data_base_dir = 'algo/data'
df_202209 = pd.read_csv(f'{data_base_dir}/market_data_binance.by_minute_ALL_2022-09-01T04:00:00Z_2022-09-30T03:59:00Z.csv')
df_202209['time'] = pd.to_datetime(df_202209['timestamp'], unit='s')
df_close_202209 = df_202209.pivot(index='time', columns='symbol', values='close')

df_202308 = pd.read_csv(f'{data_base_dir}/market_data_binance.by_minute_ALL_2023-08-01T04:00:00Z_2023-08-31T03:59:00Z.csv')
df_202308['time'] = pd.to_datetime(df_202308['timestamp'], unit='s')
df_close_202308 = df_202308.pivot(index='time', columns='symbol', values='close')
df_close_20230806_20230809 = df_close_202308[(df_close_202308.index >= date_str_20230806) & (df_close_202308.index < date_str_20230809)]

def get_close_between_datetime(sample_period_minutes, symbols, start_datetime_str, end_datetime_str):  
    df = df_close_202209 # df_close
    df1 = df[(df.index >= start_datetime_str) & (df.index < end_datetime_str)][symbols].resample(f'{sample_period_minutes}min').last().dropna()
    df = df_close_202308 # df_close
    df2 = df[(df.index >= start_datetime_str) & (df.index < end_datetime_str)][symbols].resample(f'{sample_period_minutes}min').last().dropna()
    return pd.concat([df1, df2])

def get_close_between_date(sample_period_minutes, symbols, start_date_str, end_date_str):   
    return get_close_between_datetime(sample_period_minutes, symbols, start_date_str + " 00:00:000", end_date_str + " 00:00:000")

def get_high_corr_symbols(start_symbol, num, candidate_symbols):
    df_corr = df_close_202308[(df_close_202308.index >= date_str_20230806) & (df_close_202308.index < date_str_20230809)].resample(f'10min').last().corr()

    def best_corr(sym, symbols):
        corrs = []
        for symbol_col in symbols:
            if sym == symbol_col: break
            corrs.append((df_corr.loc[sym][symbol_col], sym, symbol_col,))
        
        best_corr = sorted(corrs, reverse=True)[0]
        print(f'{best_corr}')
        return best_corr[2]

    ret = [start_symbol]
    symbols = [s for s in candidate_symbols if s != start_symbol]
    symbol_base = start_symbol
    for _ in range(num-1):
        symbols = [s for s in symbols if s != symbol_base]
        symbol = best_corr(symbol_base, symbols)
        ret.append(symbol)
        symbol_base = symbol

    return ret

def get_high_corr_symbols_set_of(set_size, set_num, candidate_symbols):
    df_corr = df_close_202308[(df_close_202308.index >= date_str_20230806) & (df_close_202308.index < date_str_20230809)].resample(f'10min').last().corr()

    def find_start_symbol(symbols_pool):
        corrs = []
        for symbol_row in symbols_pool:
            for symbol_col in symbols_pool:
                if symbol_row == symbol_col: break
                corrs.append((df_corr.loc[symbol_row][symbol_col], symbol_row, symbol_col,))

        corrs = sorted(corrs, reverse=True)
        start_symbol = corrs[0][1]
        return start_symbol

    start_symbol = find_start_symbol(candidate_symbols)
    ret = []
    symbols_pool = [s for s in candidate_symbols if s != start_symbol]
    for _ in range(set_num):
        print(f'start_symbol: {start_symbol}')
        symbols = get_high_corr_symbols(start_symbol, set_size, symbols_pool)
        ret.append(symbols)

        symbols_pool = [s for s in symbols_pool if s not in symbols]
        start_symbol = find_start_symbol(symbols_pool)
        symbols_pool = [s for s in symbols_pool if s != start_symbol]
    return ret

symbols_pool = [s for _, s in sorted([(m, s) for s, m in df_close_20230806_20230809.mean().items() if m > 50], reverse=True)]
# remove duplicates within USDT / BUSD swap
symbols_pool = [s for s in symbols_pool if s not in [s.replace('USDT', 'BUSD') for s in symbols_pool if 'USDT' in s]]

symbols_sets = get_high_corr_symbols_set_of(3, 4, symbols_pool)
symbols_sets

#symbols = ['YFIIUSDT', 'ETHUSDT', 'PAXGUSDT', 'BIFIUSDT', 'BNBUSDT']
symbols = ['YFIIUSDT', 'ETHUSDT', 'BIFIUSDT']
#symbols = ['GMXUSDT', 'BTCUSDT', 'MKRUSDT']