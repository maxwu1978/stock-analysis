"""加密货币分形调研 — 日线级 MF-DFA 覆盖主流币种

目标: 看 BTC 的 asym=+0.727 / Δα=1.44 特征是加密市场普遍规律,
      还是 BTC 独有.
      同时记录是否都是"分形超强→信号失效"的陷阱.

币种选择 (按市值排序):
  BTC / ETH / SOL / BNB / XRP / ADA / DOGE / AVAX / DOT / LINK
"""

import sys
from datetime import datetime

import numpy as np
import pandas as pd
import yfinance as yf

from fractal_survey import mfdfa_spectrum


CRYPTO_UNIVERSE = {
    "BTC-USD": "比特币 Bitcoin",
    "ETH-USD": "以太坊 Ethereum",
    "SOL-USD": "Solana",
    "BNB-USD": "BNB",
    "XRP-USD": "XRP",
    "ADA-USD": "Cardano",
    "DOGE-USD": "狗狗币 Dogecoin",
    "AVAX-USD": "Avalanche",
    "DOT-USD": "Polkadot",
    "LINK-USD": "Chainlink",
    "MATIC-USD": "Polygon",
    "LTC-USD": "莱特币 Litecoin",
}


def survey(days: int = 400, window: int = 120) -> pd.DataFrame:
    """对每个币种跑 MF-DFA 并返回统计."""
    rows = []
    for sym, name in CRYPTO_UNIVERSE.items():
        print(f"  {sym:<12} ({name})...", end="", flush=True)
        try:
            t = yf.Ticker(sym)
            hist = t.history(period=f"{max(days, 400)}d", interval="1d", auto_adjust=False)
            if hist.index.tz is not None:
                hist.index = hist.index.tz_localize(None)
            if len(hist) < window + 20:
                print(f" 数据不足 ({len(hist)})")
                continue
            closes = hist["Close"].astype(float)
            log_ret = np.log(closes / closes.shift(1))
            spec = mfdfa_spectrum(log_ret.iloc[-window:])
            if not spec:
                print(" MF-DFA 失败")
                continue

            # 波动率
            vol_ann = log_ret.iloc[-20:].std() * np.sqrt(365) * 100
            # 近期涨跌
            chg_30d = (closes.iloc[-1] / closes.iloc[-30] - 1) * 100 if len(closes) > 30 else np.nan
            chg_180d = (closes.iloc[-1] / closes.iloc[-180] - 1) * 100 if len(closes) > 180 else np.nan

            spec.update({
                "symbol": sym,
                "name": name,
                "last_price": float(closes.iloc[-1]),
                "vol_20d_ann": vol_ann,
                "chg_30d": chg_30d,
                "chg_180d": chg_180d,
                "n_samples": len(closes),
            })
            rows.append(spec)
            print(f" ok (asym={spec['asym']:+.3f})")
        except Exception as e:
            print(f" ERR: {str(e)[:40]}")
    return pd.DataFrame(rows)


def report(df: pd.DataFrame) -> None:
    if df.empty:
        print("无数据.")
        return

    A_BASE = {"asym": 0.222, "hq_neg4": 0.683, "hq_pos4": 0.461}
    US_BASE = {"asym": 0.225, "hq_neg4": 0.741, "hq_pos4": 0.516}

    print()
    print("═" * 92)
    print(f"  加密货币 MF-DFA 谱 vs A股/美股基准  (n={len(df)})")
    print("═" * 92)
    print()

    # 逐币种 (按 asym 降序)
    df_s = df.sort_values("asym", ascending=False)
    cols = ["symbol", "name", "last_price", "chg_30d", "chg_180d", "vol_20d_ann",
            "delta_alpha", "alpha0", "hq2", "hq_neg4", "hq_pos4", "asym"]
    print("  [按 asym 降序]")
    print(df_s[[c for c in cols if c in df_s.columns]].to_string(index=False,
          float_format=lambda x: f"{x:+.2f}" if abs(x) < 10 else f"{x:.0f}"))
    print()

    # 对比基准
    print("  [均值对比]")
    print(f"    h(q=-4) 加密均值 {df['hq_neg4'].mean():+.3f}  (A股 {A_BASE['hq_neg4']:+.3f}, 美股 {US_BASE['hq_neg4']:+.3f})")
    print(f"    h(q=+4) 加密均值 {df['hq_pos4'].mean():+.3f}  (A股 {A_BASE['hq_pos4']:+.3f}, 美股 {US_BASE['hq_pos4']:+.3f})")
    print(f"    asym    加密均值 {df['asym'].mean():+.3f}  (A股 {A_BASE['asym']:+.3f}, 美股 {US_BASE['asym']:+.3f})")
    print()

    # 触发频率分析 (用 asym 是否超 0.3 / 0.5 作为触发门槛)
    n = len(df)
    print("  [分形信号触发带宽分布]")
    print(f"    asym 0.15-0.30 (美股均值区间): {(df['asym'].between(0.15, 0.30)).sum()}/{n}")
    print(f"    asym 0.30-0.50 (最佳带宽):    {(df['asym'].between(0.30, 0.50)).sum()}/{n}")
    print(f"    asym 0.50-0.65 (偏强):        {(df['asym'].between(0.50, 0.65)).sum()}/{n}")
    print(f"    asym > 0.65 (超强/信号泛滥?): {(df['asym'] > 0.65).sum()}/{n}")
    print()

    # 波动率对比
    print("  [波动率对比]")
    print(f"    加密均值 {df['vol_20d_ann'].mean():.0f}%   max={df['vol_20d_ann'].max():.0f}% ({df.loc[df['vol_20d_ann'].idxmax(), 'symbol']})")
    print(f"    min={df['vol_20d_ann'].min():.0f}% ({df.loc[df['vol_20d_ann'].idxmin(), 'symbol']})")
    print(f"    对比美股科技股 ~30-50%, 铂金 ~40%, 美股期货 ~18-50%")
    print()

    # 结论
    n_extreme = (df["asym"] > 0.65).sum()
    print("  [结论]")
    if n_extreme / n > 0.5:
        print(f"    {n_extreme}/{n} 币种 asym > 0.65, 分形超强是加密货币普遍特征")
        print(f"    BTC 的策略失败很可能在其他币种上同样失败 (信号触发过频)")
    elif n_extreme / n > 0.2:
        print(f"    {n_extreme}/{n} 币种 asym > 0.65, 加密市场部分币种分形超强")
        print(f"    asym 适中的币种 (0.3-0.5) 可能更适合传统分形反转模型")
    else:
        print(f"    只 {n_extreme}/{n} 币种 asym > 0.65, BTC 可能是异常值")
        print(f"    其他加密货币可能适用传统分形模型")


if __name__ == "__main__":
    print(f"\n{'═' * 72}")
    print(f"  加密货币 MF-DFA 分形调研  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("═" * 72)
    print()

    df = survey(days=400, window=120)
    report(df)

    out = f"crypto_fractal_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    df.to_csv(out, index=False)
    print(f"\n  详细: {out}\n")
