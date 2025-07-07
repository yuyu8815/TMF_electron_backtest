from Backtestingtools.trade_loader import build_df_dict2
from Backtestingtools.electron_pnl_analyzer import TMFPnLAnalyzer
import datetime as dt

def parse_comma_date(date_str):
    parts = [int(x) for x in date_str.strip().split(",")]
    return dt.date(*parts)

if __name__ == "__main__":
    start_str = input("開始日期（格式：YYYY,M,D，例如 2025,3,1）： ")
    end_str = input("結束日期（格式：YYYY,M,D，例如 2025,3,25）： ")

    start_date = parse_comma_date(start_str)
    end_date = parse_comma_date(end_str)

    df_dict = build_df_dict2(start=start_date, end=end_date)
    analyzer = TMFPnLAnalyzer(df_dict)

    analyzer.compute_daily_pnl()
    analyzer.print_daily_report()

    # 🔹 找出虧損最慘的日子
    print("\n虧損最慘的 5 天：")
    worst_days = analyzer.get_top_n_worst_days(n=5)
    for i, (d, pnl) in enumerate(worst_days, 1):
        print(f"{i}. {d} → {pnl:,.2f} TWD")

    # 🔹 使用者挑一天畫圖
    choice_str = input("\n請選擇要畫圖的日期（格式：YYYY,M,D，例如 2025,3,5）： ")
    choice_date = parse_comma_date(choice_str)
    analyzer.plot_single_day(choice_date)
