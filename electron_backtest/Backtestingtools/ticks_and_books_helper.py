import tetrion.commands as cmd
import pandas as pd

def load_ticks_and_books(products, date, source, depth=5):
    """
    給定商品列表與日期，回傳 {product: {'book': df_book, 'tick': df_tick}} dict
    """
    result = {}
    for p in products:
        key = f'FUT_TAIFEX_{p}:{date.strftime("%Y%m")}'
        df_book = cmd.book_printer_v2(key, date, source=source, depth=depth)
        df_tick = cmd.tick_printer(key, date)
        result[p] = {"book": df_book, "tick": df_tick}
    return result

def query_between(df, timestamp, window_ms=3000):
    """
    傳入 dataframe 與某個時間點，抓前後 window_ms 毫秒內的資料
    """
    start = timestamp - pd.Timedelta(milliseconds=window_ms)
    end = timestamp + pd.Timedelta(milliseconds=window_ms)
    return df.loc[(df.index >= start) & (df.index <= end)]

class MarketDataAnalyzer:
    def __init__(self, raw_data):
        """
        raw_data: { product: { "book": df_book, "tick": df_tick } }
        """
        self.raw_data = raw_data
    def query_book_between(df_book, timestamp, window_ms=500):
        if not isinstance(df_book.index, pd.DatetimeIndex):
            df_book.index = pd.to_datetime(df_book.index)

        start = timestamp - pd.Timedelta(milliseconds=window_ms)
        end = timestamp + pd.Timedelta(milliseconds=window_ms)

        return df_book.loc[(df_book.index >= start) & (df_book.index <= end)]

    def query_ticks_between(self, product, timestamp, window_ms=1000):
        """
        找 timestamp 前後 window_ms 毫秒範圍內的成交 tick
        """
        df_tick = self.raw_data[product]["tick"]
        if df_tick.empty:
            print(f"⚠ {product} 沒有 tick 資料")
            return df_tick

        if not isinstance(df_tick.index, pd.DatetimeIndex):
            df_tick.index = pd.to_datetime(df_tick.index)

        start = timestamp - pd.Timedelta(milliseconds=window_ms)
        end = timestamp + pd.Timedelta(milliseconds=window_ms)
        return df_tick.loc[(df_tick.index >= start) & (df_tick.index <= end)]
