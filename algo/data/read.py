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
date_str_20230810 = "2023-08-10"
date_str_20230811 = "2023-08-11"
date_str_20230812 = "2023-08-12"
date_str_20230813 = "2023-08-13"
date_str_20230814 = "2023-08-14"
date_str_20230815 = "2023-08-15"
date_str_20230816 = "2023-08-16"
date_str_20230831 = "2023-08-31"

date_str_20230901 = "2023-09-01"
date_str_20230930 = "2023-09-30"


base_binance = 'algo/data/binance'
df_binance_202209 = pd.read_parquet(f'{base_binance}/df_close_202209.parquet')
df_binance_202308 = pd.read_parquet(f'{base_binance}/df_close_202308.parquet')
df_binance_202309 = pd.read_parquet(f'{base_binance}/df_close_202309.parquet')


base_gemini = 'algo/data/gemini'
df_gemini_202309 = pd.read_parquet(f'{base_gemini}/df_gemini_202309.parquet')
df_gemini_202310 = pd.read_parquet(f'{base_gemini}/df_gemini_202310.parquet')


base_kraken = 'algo/data/kraken'
df_kraken_202309 = pd.read_parquet(f'{base_kraken}/df_kraken_202309.parquet')
df_kraken_202310 = pd.read_parquet(f'{base_kraken}/df_kraken_202310.parquet')



def get_close_between_datetime(df, sample_period_minutes, symbols, start_datetime_str, end_datetime_str, if_2023=True):
    df_between = df[(df.index >= start_datetime_str) & (df.index < end_datetime_str)][symbols].resample(f'{sample_period_minutes}min').last().dropna()
    return df_between


def get_close_between_date(df, sample_period_minutes, symbols, start_date_str, end_date_str, if_2023=True):   
    return get_close_between_datetime(df, sample_period_minutes, symbols, start_date_str + " 00:00:000", end_date_str + " 00:00:000", if_2023=if_2023)


def get_high_corr_symbols(df, sample_resolution_minutes, start_symbol, num, candidate_symbols, high_corr=True):
    df_corr = df.resample(f'{sample_resolution_minutes}min').last().corr()

    def best_corr(sym, symbols):
        corrs = []
        for symbol_col in symbols:
            if sym == symbol_col: break
            corrs.append((df_corr.loc[sym][symbol_col], sym, symbol_col,))

        abs_corrs = [(abs(c[0]), c[1], c[2],) for c in corrs]
        pick = sorted(abs_corrs, reverse=high_corr)[0]
        print(f'{pick}')
        return pick[2]

    ret = [start_symbol]
    symbols = [s for s in candidate_symbols if s != start_symbol]
    symbol_base = start_symbol
    for _ in range(num-1):
        symbols = [s for s in symbols if s != symbol_base]
        symbol = best_corr(symbol_base, symbols)
        ret.append(symbol)
        symbol_base = symbol

    return ret


def get_high_corr_symbols_set_of(df, sample_resolution_minutes, set_size, set_num, candidate_symbols, high_corr=True):
    '''
    high_corr: True if getting highest, False if getting lowest.
    '''
    df_corr = df.resample(f'{sample_resolution_minutes}min').last().corr()

    def find_start_symbol(symbols_pool):
        corrs = []
        for symbol_row in symbols_pool:
            for symbol_col in symbols_pool:
                if symbol_row == symbol_col: break
                corrs.append((df_corr.loc[symbol_row][symbol_col], symbol_row, symbol_col,))

        abs_corrs = [(abs(c[0]), c[1], c[2],) for c in corrs]
        pick = sorted(abs_corrs, reverse=high_corr)[0]
        start_symbol = pick[1]
        return start_symbol

    start_symbol = find_start_symbol(candidate_symbols)
    ret = []
    symbols_pool = [s for s in candidate_symbols if s != start_symbol]
    for _ in range(set_num):
        print(f'start_symbol: {start_symbol}')
        symbols = get_high_corr_symbols(df, sample_resolution_minutes, start_symbol, set_size, symbols_pool, high_corr=high_corr)
        ret.append(symbols)

        symbols_pool = [s for s in symbols_pool if s not in symbols]
        start_symbol = find_start_symbol(symbols_pool)
        symbols_pool = [s for s in symbols_pool if s != start_symbol]
    return ret








