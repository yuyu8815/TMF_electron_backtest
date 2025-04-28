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


def build_df_dict(
    start,
    end,
    strat="capital_electron_tmf",
    acc="TAIFEX100",
    redishost="prod1.capital.radiant-knight.com"
):
    """
    回傳 df_dict: {date -> DataFrame}

    start, end:
        可接受格式：
        - datetime.date
        - 'YYYY-MM-DD'
        - 'YYYY,M,D'（例如 '2025,4,1'）
    """
    def parse_date(x):
        if isinstance(x, str):
            if "," in x:
                parts = [int(p) for p in x.split(",")]
                return dt.date(*parts)
            return dt.datetime.strptime(x, "%Y-%m-%d").date()
        return x

    start = parse_date(start)
    end = parse_date(end)

    df_dict = {}

    for d in pd.date_range(start=start, end=end).to_pydatetime():
        d = d.date()
        dfs = []

        for night in (True, False):  # 夜盤先抓
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
            # print(f"No data for {d}")
            None

    return df_dict
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
