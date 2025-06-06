import redis
import json
import pandas as pd
import datetime as dt
from tqdm import tqdm  

def get_fills_from_redis(
    strat='capital_txo_main',
    acc='TAIFEX100',
    date=dt.date.today(),
    night_session=True,
    redishost='prod1.capital.radiant-knight.com'
):
    r = redis.StrictRedis(host=redishost, port=6379, db=0)
    rk = '{}:{}'.format(acc, date.strftime('%Y%m%d'))
    rk_strat = '{}:{}'.format(strat, date.strftime('%Y%m%d'))

    if night_session:
        rk = rk + 'E'
        rk_strat = rk_strat + 'E'

    keys = [k.decode() for k in r.keys()]
    if rk not in keys:
        None
        if rk_strat in keys:
            rk = rk_strat
        else:
            return None

    msgs = r.lrange(rk, 0, -1)
    trd_df = pd.DataFrame([json.loads(m.decode()) for m in msgs])

    if trd_df.empty:
        print(f'Empty redis key: {rk}')
        return None

    if trd_df['ts'][0] > 1e11:
        trd_df['time'] = trd_df['ts'].apply(lambda x: dt.datetime.fromtimestamp(x / 1e6))
    else:
        trd_df['time'] = trd_df['ts'].apply(lambda x: dt.datetime.fromtimestamp(x))

    trd_df.set_index('time', inplace=True)
    return trd_df


def parse_comma_date(date_str):
    parts = [int(x) for x in date_str.strip().split(",")]
    return dt.date(*parts)

def build_df_dict2(
    start,
    end,
    strat="capital_electron_tmf",
    acc="TAIFEX100",
    redishost="prod1.capital.radiant-knight.com"
):
    if isinstance(start, str):
        start = parse_comma_date(start)
    if isinstance(end, str):
        end = parse_comma_date(end)

    df_dict = {}
    date_list = pd.date_range(start=start, end=end).to_pydatetime()

    for d in tqdm(date_list, desc="Loading fills by day"):
        d = d.date()
        dfs = []

        for night in (True, False):
            df = get_fills_from_redis(
                strat=strat,
                acc=acc,
                date=d,
                night_session=night,
                redishost=redishost
            )
            if df is not None and not df.empty:
                dfs.append(df.reset_index())

        if dfs:
            full = pd.concat(dfs, ignore_index=True)
            full["time"] = pd.to_datetime(full["time"])
            full = full.sort_values("time").reset_index(drop=True)
            df_dict[d] = full
        else:
            print(f"No data for {d}")
            # None

    return df_dict
def concat_df(
    start,
    end,
    strat="capital_electron_tmf",
    acc="TAIFEX100",
    redishost="prod1.capital.radiant-knight.com"
) -> pd.DataFrame:
    if isinstance(start, str):
        start = parse_comma_date(start)
    if isinstance(end, str):
        end = parse_comma_date(end)

    date_list = pd.date_range(start=start, end=end).to_pydatetime()

    dfs = []

    for d in tqdm(date_list, desc="Concatenating fills"):
        d = d.date()
        for night in (True, False):
            df = get_fills_from_redis(
                strat=strat,
                acc=acc,
                date=d,
                night_session=night,
                redishost=redishost
            )
            if df is not None and not df.empty:
                dfs.append(df.reset_index())

    if not dfs:
        print("No data found in given date range.")
        return pd.DataFrame()

    df_all = pd.concat(dfs, ignore_index=True)
    df_all["time"] = pd.to_datetime(df_all["time"])
    df_all = df_all.sort_values("time").reset_index(drop=True)
    return df_all
import pandas as pd
import datetime as dt
import redis
import json
from tqdm import tqdm


class TradeLoader:
    def __init__(self, strat="capital_electron_tmf", acc="TAIFEX100", redishost="prod1.capital.radiant-knight.com"):
        self.strat = strat
        self.acc = acc
        self.redishost = redishost

    def _get_redis_connection(self):
        return redis.StrictRedis(host=self.redishost, port=6379, db=0)

    def _fetch_one_day(self, date: dt.date, night_session=True):
        r = self._get_redis_connection()

        rk = f"{self.acc}:{date.strftime('%Y%m%d')}"
        rk_strat = f"{self.strat}:{date.strftime('%Y%m%d')}"

        if night_session:
            rk += "E"
            rk_strat += "E"

        keys = [k.decode() for k in r.keys()]
        if rk not in keys:
            if rk_strat in keys:
                rk = rk_strat
            else:
                return None

        msgs = r.lrange(rk, 0, -1)
        df = pd.DataFrame([json.loads(m.decode()) for m in msgs])
        if df.empty:
            return None

        if df['ts'][0] > 1e11:
            df['time'] = df['ts'].apply(lambda x: dt.datetime.fromtimestamp(x / 1e6))
        else:
            df['time'] = df['ts'].apply(lambda x: dt.datetime.fromtimestamp(x))
        df.set_index('time', inplace=True)
        return df

    def load_single_day(self, date):
        if isinstance(date, str):
            date = pd.to_datetime(date).date()

        dfs = []
        for night in (True, False):
            df = self._fetch_one_day(date, night_session=night)
            if df is not None and not df.empty:
                dfs.append(df)

        if not dfs:
            return pd.DataFrame()

        full = pd.concat(dfs)
        full.index = pd.to_datetime(full.index)
        full = full.sort_index()
        return full

    def load_concat(self, start, end):
        if isinstance(start, str):
            start = pd.to_datetime(start).date()
        if isinstance(end, str):
            end = pd.to_datetime(end).date()

        date_list = pd.date_range(start=start, end=end).to_pydatetime()
        dfs = []

        for d in tqdm(date_list, desc=f"Loading fills for {self.strat}"):
            d = d.date()
            df_day = self.load_single_day(d)
            if not df_day.empty:
                dfs.append(df_day)

        if not dfs:
            return pd.DataFrame()

        df_all = pd.concat(dfs)
        df_all.index = pd.to_datetime(df_all.index)
        return df_all.sort_index()

    def load_by_day(self, start, end):
        if isinstance(start, str):
            start = pd.to_datetime(start).date()
        if isinstance(end, str):
            end = pd.to_datetime(end).date()

        date_list = pd.date_range(start=start, end=end).to_pydatetime()
        df_dict = {}

        for d in tqdm(date_list, desc=f"Loading daily fills for {self.strat}"):
            d = d.date()
            df_day = self.load_single_day(d)
            if not df_day.empty:
                df_dict[d] = df_day

        return df_dict

class TradeLoaderNeutrino:
    def __init__(self, strat="capital_electron_tmf", acc="TAIFEX100", redishost="prod1.capital.radiant-knight.com"):
        self.strat = strat
        self.acc = acc
        self.redishost = redishost

    def _get_redis_connection(self):
        return redis.StrictRedis(host=self.redishost, port=6379, db=0)

    def _fetch_one_day(self, date: dt.date, night_session=True):
        r = self._get_redis_connection()

        rk = f"{self.acc}:{date.strftime('%Y%m%d')}"
        rk_strat = f"{self.strat}:{date.strftime('%Y%m%d')}"

        if night_session:
            rk += "E"
            rk_strat += "E"

        keys = [k.decode() for k in r.keys()]
        if rk not in keys:
            if rk_strat in keys:
                rk = rk_strat
            else:
                return None

        msgs = r.lrange(rk, 0, -1)
        df = pd.DataFrame([json.loads(m.decode()) for m in msgs])
        if df.empty:
            return None

        if df['ts'][0] > 1e11:
            df['time'] = df['ts'].apply(lambda x: dt.datetime.fromtimestamp(x / 1e6))
        else:
            df['time'] = df['ts'].apply(lambda x: dt.datetime.fromtimestamp(x))
        df.set_index('time', inplace=True)
        return df

    def load_single_day(self, date):
        if isinstance(date, str):
            date = pd.to_datetime(date).date()

        dfs = []
        for night in (True, False):
            df = self._fetch_one_day(date, night_session=night)
            if df is not None and not df.empty:
                dfs.append(df)

        if not dfs:
            return pd.DataFrame()

        full = pd.concat(dfs)
        full.index = pd.to_datetime(full.index)
        full = full.sort_index()
        return full

    def load_concat(self, start, end):
        if isinstance(start, str):
            start = pd.to_datetime(start).date()
        if isinstance(end, str):
            end = pd.to_datetime(end).date()

        date_list = pd.date_range(start=start, end=end).to_pydatetime()
        dfs = []

        for d in tqdm(date_list, desc=f"Loading fills for {self.strat}"):
            d = d.date()
            df_day = self.load_single_day(d)
            if not df_day.empty:
                dfs.append(df_day)

        if not dfs:
            return pd.DataFrame()

        df_all = pd.concat(dfs)
        df_all.index = pd.to_datetime(df_all.index)
        return df_all.sort_index()

    def load_by_day(self, start, end):
        if isinstance(start, str):
            start = pd.to_datetime(start).date()
        if isinstance(end, str):
            end = pd.to_datetime(end).date()

        date_list = pd.date_range(start=start, end=end).to_pydatetime()
        df_dict = {}

        for d in tqdm(date_list, desc=f"Loading daily fills for {self.strat}"):
            d = d.date()
            df_day = self.load_single_day(d)
            if not df_day.empty:
                df_dict[d] = df_day

        return df_dict

