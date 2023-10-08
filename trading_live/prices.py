import pandas as pd, numpy as np


class PriceCache:
    def __init__(self, symbols, windows_minutes):
        self.symbols = symbols
        self.windows_minutes = windows_minutes
        self.df_prices = None

    def get_df_prices(self):
        return self.df_prices



