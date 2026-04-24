"""美股期货 MF-DFA 分形结构调研

目标: 用 yfinance 拉取美股期货主力连续合约日线,
     对每个品种计算 MF-DFA 谱 (Δα/α_0/h(q=2)/h(q=-4)/h(q=+4)/asym),
     验证分形非对称是否在期货市场同样稳健.

使用:
  python futures_fractal_survey.py           # 默认: CME 指数 + 微型
  python futures_fractal_survey.py all       # 全部 19 个品种
  python futures_fractal_survey.py metals    # 贵金属
"""

import argparse
import numpy as np
import pandas as pd
from datetime import datetime

from fetch_futures_yf import fetch_all_futures, FUTURES_UNIVERSE
from fractal_survey import mfdfa_spectrum


def survey(universe: str = "cme_indexes", days: int = 400, window: int = 120) -> pd.DataFrame:
    """对一组期货做 MF-DFA 横截面."""
    print(f"[1/2] 拉取 universe={universe!r} {days}天历史...")
    hist = fetch_all_futures(days=days, universe=universe)
    if not hist:
        return pd.DataFrame()

    print(f"\n[2/2] 计算 MF-DFA 谱 (window={window})...")
    rows = []
    for symbol, df in hist.items():
        name = FUTURES_UNIVERSE[universe].get(symbol) if universe != "all" else FUTURES_UNIVERSE["all"].get(symbol, symbol)
        closes = df["close"].astype(float)
        log_ret = np.log(closes / closes.shift(1))
        if len(log_ret) < window:
            print(f"  {symbol}: 数据不足 ({len(log_ret)} < {window})")
            continue
        spec = mfdfa_spectrum(log_ret.iloc[-window:])
        if not spec:
            continue
        spec["symbol"] = symbol
        spec["name"] = name
        # 近期表现 (对照 A 股 5 只的"涨跌/波动率")
        spec["last_close"] = closes.iloc[-1]
        spec["chg_20d"] = (closes.iloc[-1] / closes.iloc[-20] - 1) * 100 if len(closes) >= 20 else np.nan
        spec["vol_20d_ann"] = log_ret.iloc[-20:].std() * np.sqrt(252) * 100
        rows.append(spec)

    return pd.DataFrame(rows)


def report(df: pd.DataFrame, label: str = "") -> None:
    """打印横截面报告 + 和 A 股 50 只基准对比."""
    if df.empty:
        print("无数据.")
        return

    # A 股基准 (来自前轮大样本调研, n=50)
    A_BASE = {"hq_neg4": 0.683, "hq_pos4": 0.461, "asym": 0.222, "asym_pos_pct": 92.0}

    n = len(df)
    print()
    print("═" * 92)
    print(f"  美股期货 MF-DFA 谱调研 {label}  n={n}")
    print("═" * 92)
    print()
    print("  [逐品种 按 asym 降序]")
    cols_show = ["symbol", "name", "last_close", "chg_20d", "vol_20d_ann",
                 "delta_alpha", "alpha0", "hq2", "hq_neg4", "hq_pos4", "asym"]
    cols_show = [c for c in cols_show if c in df.columns]
    df_sorted = df.sort_values("asym", ascending=False)
    print(df_sorted[cols_show].to_string(index=False, float_format=lambda x: f"{x:+.3f}" if abs(x) < 10 else f"{x:.1f}"))
    print()

    # 统计汇总
    print("  [汇总]")
    print(f"    h(q=-4) 均值 {df['hq_neg4'].mean():+.3f} (A股基准 {A_BASE['hq_neg4']:+.3f}, 差 {df['hq_neg4'].mean() - A_BASE['hq_neg4']:+.3f})")
    print(f"    h(q=+4) 均值 {df['hq_pos4'].mean():+.3f} (A股基准 {A_BASE['hq_pos4']:+.3f}, 差 {df['hq_pos4'].mean() - A_BASE['hq_pos4']:+.3f})")
    print(f"    asym 均值   {df['asym'].mean():+.3f} (A股基准 {A_BASE['asym']:+.3f}, 差 {df['asym'].mean() - A_BASE['asym']:+.3f})")

    asym_pos = (df["asym"] > 0).sum()
    asym_strong = (df["asym"] > 0.1).sum()
    print(f"    asym > 0    {asym_pos}/{n} = {asym_pos/n*100:.0f}% (A股 {A_BASE['asym_pos_pct']:.0f}%)")
    print(f"    asym > 0.1  {asym_strong}/{n} = {asym_strong/n*100:.0f}%")
    print()
    print(f"    Δα 均值     {df['delta_alpha'].mean():+.3f} (波动结构不均匀度)")
    print(f"    α_0 均值    {df['alpha0'].mean():+.3f} (谱中心, 典型长程依赖)")
    print(f"    h(q=2) 均值 {df['hq2'].mean():+.3f} (稳健 Hurst)")
    print()

    # 与 A 股模式对比诊断
    print("  [与 A 股分形模式对比]")
    if df["asym"].mean() > 0.1 and asym_pos / n > 0.7:
        print(f"    ✓ 期货也遵循 'asym > 0' 模式 — 与 A 股和美股大盘股一致")
        print(f"      小波动段持续性 > 大波动段 (典型反转模型适用)")
    elif df["asym"].mean() < -0.1:
        print(f"    ⚠ 期货呈现 'asym < 0' 反向模式!")
        print(f"      大波动段更持续 - 需要趋势延续模型")
    else:
        print(f"    ~ 期货分形非对称弱/混合, 各品种差异大 (见上表)")

    print()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Survey MF-DFA fractal structure across futures universes")
    parser.add_argument("universe", nargs="?", default="cme_indexes", choices=sorted(FUTURES_UNIVERSE))
    parser.add_argument("--days", type=int, default=400)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    universe = args.universe

    if universe == "all":
        # 全部一起跑 + 按板块分组诊断
        full = survey("all", days=args.days)
        report(full, f"(universe=all)")
        # 按板块分组
        sector_map = {}
        for section, d in FUTURES_UNIVERSE.items():
            if section == "all":
                continue
            for sym in d:
                sector_map[sym] = section
        full["sector"] = full["symbol"].map(sector_map)
        print("\n  [按板块汇总 asym 均值]")
        grouped = full.groupby("sector")["asym"].agg(["mean", "count"])
        print(grouped.to_string(float_format=lambda x: f"{x:+.3f}"))
    else:
        df = survey(universe, days=args.days)
        report(df, f"(universe={universe})")

    # 保存 CSV
    out_path = f"futures_fractal_{universe}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    if universe == "all":
        full.to_csv(out_path, index=False)
    else:
        df.to_csv(out_path, index=False)
    print(f"\n  保存: {out_path}\n")


if __name__ == "__main__":
    main()
