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
    def detect_trade_through(self, product: str, merge_window_ms: int = 1000):
        """
        Detects trade-through events from tick data using price vs bid/ask comparison.
        Events within merge_window_ms and same direction are merged into a group.
        """
        df_tick = self.raw_data[product]["tick"]
        results = []

        for time, row in df_tick.iterrows():
            price = row.get("px")
            bid_px = row.get("bid_px")
            ask_px = row.get("ask_px")

            if pd.isna(price) or bid_px == 0 or ask_px == 0:
                continue

            if price > ask_px:
                results.append((time, price, ask_px, price - ask_px, "buy_through"))
            elif price < bid_px:
                results.append((time, price, bid_px, bid_px - price, "sell_through"))

        if not results:
            return []

        merged = []
        last_time, last_type = None, None
        group = []

        for event in results:
            t, price, ref_px, diff, typ = event

            if last_time is None:
                group = [event]
            elif (typ != last_type) or ((t - last_time).total_seconds() * 1000 > merge_window_ms):
                merged.append(group)
                group = [event]
            else:
                group.append(event)

            last_time = t
            last_type = typ

        if group:
            merged.append(group)

        summarized = []
        for group in merged:
            t0 = group[0][0]
            tn = group[-1][0]
            n = len(group)
            direction = group[0][4]
            avg_diff = sum(e[3] for e in group) / n
            summarized.append({
                "start_time": t0,
                "end_time": tn,
                "count": n,
                "direction": direction,
                "avg_diff": avg_diff,
            })

        return summarized
    def __init__(self, raw_data):
        """
        raw_data: { product: { "book": df_book, "tick": df_tick } }
        """
        self.raw_data = raw_data

    def query_book_between(df_book, timestamp, window_s=5):
        """
        使用 between_time 以「每天時間」為基準，抓 timestamp 附近的資料
        window_s 是秒數，例如 5秒
        """
        if not isinstance(df_book.index, pd.DatetimeIndex):
            df_book.index = pd.to_datetime(df_book.index)

        if df_book.empty:
            print("⚠ df_book 是空的")
            return None

        # 取出時間部分
        time_only = timestamp.time()

        # 計算 start_time 和 end_time
        start_seconds = (time_only.hour * 3600 + time_only.minute * 60 + time_only.second) - window_s
        end_seconds = (time_only.hour * 3600 + time_only.minute * 60 + time_only.second) + window_s

        start_h = int(start_seconds // 3600) % 24
        start_m = int((start_seconds % 3600) // 60)
        start_s = int(start_seconds % 60)

        end_h = int(end_seconds // 3600) % 24
        end_m = int((end_seconds % 3600) // 60)
        end_s = int(end_seconds % 60)

        start_time = f"{start_h:02}:{start_m:02}:{start_s:02}"
        end_time = f"{end_h:02}:{end_m:02}:{end_s:02}"

        df_window = df_book.between_time(start_time, end_time)

        print(f"Time：{start_time} ~ {end_time}")
        print(f" {len(df_window)} 筆資料")

        return df_window
    def query_ticks_between(self, product, timestamp, window_s=5):
        """
        找 timestamp 前後 window_ms 毫秒範圍內的成交 tick
        """
        df_tick = self.raw_data[product]["tick"]
        if df_tick.empty:
            print(f"⚠ {product} 沒有 tick 資料")
            return df_tick

        if not isinstance(df_tick.index, pd.DatetimeIndex):
            df_tick.index = pd.to_datetime(df_tick.index)

        start = timestamp - pd.Timedelta(seconds=window_s)
        end = timestamp + pd.Timedelta(seconds=window_s)
        return df_tick.loc[(df_tick.index >= start) & (df_tick.index <= end)]
class MarketDataAnalyzer2:
    def __init__(self, df_tick, df_book):
        self.df_tick = df_tick
        self.df_book = df_book

    def detect_trade_through(self, merge_window_ms=1000):
        events = []
        for t, row in self.df_tick.iterrows():
            px = row.get("px")
            bid = row.get("bid_px")
            ask = row.get("ask_px")

            if pd.isna(px) or bid == 0 or ask == 0:
                continue

            if px > ask:
                events.append((t, "buy", px - ask))
            elif px < bid:
                events.append((t, "sell", bid - px))

        if not events:
            return []

        merged = []
        group = [events[0]]

        for curr in events[1:]:
            prev = group[-1]
            gap = (curr[0] - prev[0]).total_seconds() * 1000
            if curr[1] == prev[1] and gap <= merge_window_ms:
                group.append(curr)
            else:
                merged.append(group)
                group = [curr]

        merged.append(group)

        results = []
        for g in merged:
            start = g[0][0]
            end = g[-1][0]
            direction = g[0][1]
            count = len(g)
            avg_diff = sum(x[2] for x in g) / count
            results.append({
                "start_time": start,
                "end_time": end,
                "direction": direction,
                "count": count,
                "avg_diff": avg_diff
            })

        return results
