import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt
from IPython.display import display
import tqdm  # 先加這個，若還沒安裝：pip install tqdm
class PnLAnalyzer:
    def __init__(self, df_dict):
        self.df_dict = df_dict
        self.pnl_dict = {}
    def get_top_n_worst_days(self, n=5):
        """
        回傳虧損最慘的 n 天 (由 Net PnL 排序)，格式為 [(date, total_pnl), ...]
        """
        if not self.pnl_dict:
            print("請先執行 compute_daily_pnl()")
            return []

        day_pnls = []
        for date, result in self.pnl_dict.items():
            total_pnl = sum(
                df["Net PnL (after fee)"].iloc[-1]
                for df in result["results_by_expiry"].values()
            )
            day_pnls.append((date, total_pnl))

        day_pnls.sort(key=lambda x: x[1])  # 損益由小到大排序
        return day_pnls[:n]


    def compute_daily_pnl_TMF(self):
        carry_positions = {}
        carry_prices = {}

        for date in sorted(self.df_dict.keys()):
            df = self.df_dict[date]
            result = self._compute_strategy_df(df, carry_positions, carry_prices)
            self.pnl_dict[date] = result

            carry_positions = result["final_positions"]
            carry_prices = result["final_prices"]

    def _compute_strategy_df(self, df_full: pd.DataFrame, carry_positions=None, carry_prices=None) -> dict:
        df = df_full.copy()
        df["time"] = pd.to_datetime(df["time"])
        df["product"] = df["instr"].apply(lambda x: "TXF" if "TXF" in x else ("MXF" if "MXF" in x else "TMF"))
        df["expiry"] = df["instr"].apply(lambda x: x.split(":")[-1])

        result_by_expiry = {}
        final_net_pnls = {}
        final_positions = {}
        final_prices = {}

        for expiry, group in df.groupby("expiry"):
            group = group.sort_values("time").copy()

            cumulative_equiv = carry_positions.get(expiry, 0) if carry_positions else 0
            prev_price = carry_prices.get(expiry) if carry_prices else None
            prev_equiv = cumulative_equiv
            running_pnl = 0

            records = []
            for _, row in group.iterrows():
                # Determine multipliers and base fee per product
                if row["product"] == "TMF":
                    multiplier = 10
                    base_fee = 8
                elif row["product"] == "TXF":
                    multiplier = 200
                    base_fee = 20
                else:  # MXF
                    multiplier = 50
                    base_fee = 12.5

                # Update cumulative equivalent position
                cumulative_equiv += row["sz"] * multiplier
                current_equiv = cumulative_equiv

                # Calculate PnL change based on previous price and position
                if prev_price is None:
                    pnl_change = 0
                else:
                    pnl_change = (row["px"] - prev_price) * prev_equiv
                running_pnl += pnl_change

                # Calculate fee: base fee plus per-unit fee based on price
                fee = abs(row["sz"]) * (base_fee + row["px"] * multiplier * 0.00002)

                records.append({
                    "Time": row["time"],
                    "Product": row["product"],
                    "Price": row["px"],
                    "size": row["sz"],
                    "Cumulative Position": current_equiv,
                    "PnL Change": pnl_change,
                    "Cumulative PnL": running_pnl,
                    "Fee": fee,
                })

                prev_price = row["px"]
                prev_equiv = current_equiv

            df_result = pd.DataFrame(records)
            df_result["Net PnL (after fee)"] = df_result["Cumulative PnL"] - df_result["Fee"].cumsum()
            df_result["Contract"] = expiry
            df_result = df_result[
                ["Time", "Product", "Price", "size", "Cumulative Position",
                "PnL Change", "Cumulative PnL", "Fee", "Net PnL (after fee)", "Contract"]
            ]

            result_by_expiry[expiry] = df_result
            final_net_pnls[expiry] = df_result["Net PnL (after fee)"].iloc[-1]
            final_positions[expiry] = df_result["Cumulative Position"].iloc[-1]
            final_prices[expiry] = df_result["Price"].iloc[-1]

        return {
            "results_by_expiry": result_by_expiry,
            "final_net_pnls": final_net_pnls,
            "final_positions": final_positions,
            "final_prices": final_prices
        }

    def plot_daily_pnl(self):
        for date, result in sorted(self.pnl_dict.items()):
            expiry_pnls = {}
            expiry_positions = {}

            for expiry, df_expiry in result["results_by_expiry"].items():
                final_net_pnl = df_expiry["Net PnL (after fee)"].iloc[-1]
                final_position = df_expiry["Cumulative Position"].iloc[-1]
                expiry_pnls[expiry] = final_net_pnl
                expiry_positions[expiry] = final_position

            total_pnl = sum(expiry_pnls.values())

            plt.figure(figsize=(12, 6))
            for expiry, df_expiry in result["results_by_expiry"].items():
                df_expiry = df_expiry.sort_values("Time")
                plt.plot(df_expiry["Time"], df_expiry["Net PnL (after fee)"], label=expiry)

            plt.axhline(0, color='black', linestyle='--')
            plt.title(f"{date} All Day Net PnL (After Fee) | Total: {total_pnl:,.2f} TWD")
            plt.xlabel("Time")
            plt.ylabel("Net PnL (After Fee)")
            plt.xticks(rotation=45)
            plt.legend()
            plt.grid(True)
            plt.tight_layout()
            plt.show()

            df_day = pd.concat(result["results_by_expiry"].values(), ignore_index=True)
            df_day["PnL Net"] = df_day["PnL Change"] - df_day["Fee"]
            worst = df_day.nsmallest(10, "PnL Net")[[
                "Time", "Contract", "Product", "Price", "size",
                "Cumulative Position", "PnL Change", "Fee", "PnL Net"
            ]]
            print(f"\n {date} 最虧損的10筆交易：")
            print(worst.to_string(index=False))

            print(f"\n {date} 各到期月份總損益：")
            for expiry, pnl in expiry_pnls.items():
                print(f"  {expiry}: {pnl:,.2f} TWD")

            # print(f"\n {date} 各到期月份收盤剩餘倉位：")
            # for expiry, pos in expiry_positions.items():
            #     if pos != 0:
            #         print(f"{expiry}: {pos} 等效口數 (未清倉)")
            #     else:
            #         print(f"{expiry}: {pos} 等效口數")

            print(f"\n {date} 當日總損益： {total_pnl:,.2f} TWD")
            print("="*80)

    # def check_opening_carry(self, date):
    #         today_result = self.pnl_dict.get(date)
    #         prev_date = max([d for d in self.pnl_dict.keys() if d < date], default=None)
    #         prev_result = self.pnl_dict.get(prev_date) if prev_date else None

    #         if prev_result:
    #             print(f"\n=== {prev_date} 收盤最後幾筆成交紀錄 ===")
    #             for expiry, df_expiry in prev_result["results_by_expiry"].items():
    #                 print(f"\n【{expiry}】")
    #                 display(
    #                     df_expiry[["Time", "Product", "Contract", "Price", "size", "Cumulative Position"]]
    #                     .sort_values("Time")
    #                     .tail(10)
    #                 )
    #         else:
    #             print(f"\n 找不到 {date} 前一天的成交資料")

    #         print(f"\n=== {date} 開盤前幾筆成交紀錄 ===")
    #         for expiry, df_expiry in today_result["results_by_expiry"].items():
    #             print(f"\n【{expiry}】")
    #             display(
    #                 df_expiry[["Time", "Product", "Contract", "Price", "size", "Cumulative Position"]]
    #                 .sort_values("Time")
    #                 .head(10)
    #             )

    def plot_cumulative_position(self, date):
        pnl_result = self.pnl_dict[date]
        plt.figure(figsize=(12, 6))
        for expiry, df_expiry in pnl_result["results_by_expiry"].items():
            df_expiry = df_expiry.sort_values("Time")
            plt.plot(df_expiry["Time"], df_expiry["Cumulative Position"], label=expiry)

        plt.axhline(0, color='black', linestyle='--')
        plt.title(f"{date} Cumulative Position (Each Expiry)")
        plt.xlabel("Time")
        plt.ylabel("Cumulative Position")
        plt.xticks(rotation=45)
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.show()

    def plot_single_day(self, date):
        result = self.pnl_dict[date]
        expiry_pnls = {
            expiry: df["Net PnL (after fee)"].iloc[-1]
            for expiry, df in result["results_by_expiry"].items()
        }
        total_pnl = sum(expiry_pnls.values())

        plt.figure(figsize=(12, 6))
        for expiry, df in result["results_by_expiry"].items():
            df = df.sort_values("Time")
            plt.plot(df["Time"], df["Net PnL (after fee)"], label=expiry)

        plt.title(f"{date} Net PnL | Total: {total_pnl:,.2f} TWD")
        plt.xlabel("Time")
        plt.ylabel("Net PnL (After Fee)")
        plt.xticks(rotation=45)
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.show()

    def print_single_day_report(self, date):
        result = self.pnl_dict[date]
        expiry_pnls = {}
        expiry_positions = {}

        for expiry, df in result["results_by_expiry"].items():
            expiry_pnls[expiry] = df["Net PnL (after fee)"].iloc[-1]
            expiry_positions[expiry] = df["Cumulative Position"].iloc[-1]

        total_pnl = sum(expiry_pnls.values())

        print("="*60)
        print(f"{date} 總損益：{total_pnl:,.2f} TWD")
        print("-"*60)
        print("各到期月份損益：")
        for expiry, pnl in expiry_pnls.items():
            print(f"  {expiry}: {pnl:,.2f} TWD")
        print("\n各到期月份剩餘倉位：")
        for expiry, pos in expiry_positions.items():
            status = "殘留" if pos != 0 else "✓ 清倉"
            print(f"  {expiry}: {pos} ({status})")

        df_day = pd.concat(result["results_by_expiry"].values(), ignore_index=True)
        df_day["PnL Net"] = df_day["PnL Change"] - df_day["Fee"]
        worst = df_day.nsmallest(5, "PnL Net")[[
            "Time", "Contract", "Product", "Price", "size",
            "Cumulative Position", "PnL Change", "Fee", "PnL Net"
        ]]
        print("\n最虧損交易：")
        print(worst.to_string(index=False))

    def print_daily_report(self):
        for date, result in sorted(self.pnl_dict.items()):
            expiry_pnls = {}
            expiry_positions = {}

            for expiry, df_expiry in result["results_by_expiry"].items():
                final_net_pnl = df_expiry["Net PnL (after fee)"].iloc[-1]
                final_position = df_expiry["Cumulative Position"].iloc[-1]
                expiry_pnls[expiry] = final_net_pnl
                expiry_positions[expiry] = final_position

            total_pnl = sum(expiry_pnls.values())

            # 每日總結
            print("=" * 80)
            print(f"{date} 總損益報告")
            print("-" * 80)

            print(f"各到期月份損益：")
            for expiry, pnl in expiry_pnls.items():
                print(f"  {expiry}: {pnl:,.2f} TWD")

            print(f"\n各到期月份剩餘倉位：")
            for expiry, pos in expiry_positions.items():
                status = "殘留倉位" if pos != 0 else "✓ 已清倉"
                print(f"  {expiry}: {pos} ({status})        self.get_top_n_worst_days()    ")

            print(f"\n當日總損益： {total_pnl:,.2f} TWD")

            # 最虧損的交易
            df_day = pd.concat(result["results_by_expiry"].values(), ignore_index=True)
            df_day["PnL Net"] = df_day["PnL Change"] - df_day["Fee"]
            worst = df_day.nsmallest(5, "PnL Net")[[
                "Time", "Contract", "Product", "Price", "size",
                "Cumulative Position", "PnL Change", "Fee", "PnL Net"
            ]]

            print("\n最虧損的 5 筆交易：")
            print(worst.to_string(index=False))

    def get_top_n_worst_days2(self, n=5):

        if not self.pnl_dict:
            print("請先執行 compute_daily_pnl()")
            return

        day_pnls = []
        print("\n每日總損益：")
        print("=" * 50)
        print(f"{'日期':<12} | {'總損益 (USD)':>15}")
        print("-" * 50)

        for date, result in sorted(self.pnl_dict.items(),reverse=False):
            total_pnl = sum(
                df["Net PnL (after fee)"].iloc[-1]
                for df in result["results_by_expiry"].values()
            )
            total_pnl_usd = total_pnl/ 33.0
            day_pnls.append((date, total_pnl_usd))

        # 先排序（虧損最大在最前面）
        # day_pnls.sort(key=lambda x: x[1])

        for date, pnl in day_pnls:
            print(f"{str(date):<12} | {pnl:>15,.2f}")

        # 額外列出最慘的 N 天
        sorted_by_pnl = sorted(day_pnls, key=lambda x: x[1])
        print("\n虧損最慘的前 {} 天：".format(n))
        print("=" * 50)
        for i, (date, pnl) in enumerate(sorted_by_pnl[:n], 1):
            print(f"{i}. {date} → {pnl:,.2f} USD")

    def run_full_report_TMF(self):
        self.compute_daily_pnl_TMF()
        self.get_top_n_worst_days2()
        self.plot_daily_pnl()

# testing for Github
#git add .
#git commit -m "更新了XXX"
#git push


 