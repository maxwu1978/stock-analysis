"""实盘观察脚本 — 连接 Futu OpenD, 对关注股输出实时行情 + 分形信号 + 简单技术指标.

仅读取数据, 绝不下单.
运行: python futu_live.py [--watchlist WATCHLIST_NAME]

默认关注池: 美股科技 + 港股互联网 (按前轮分形调研经验选择)

前轮发现:
  - AAPL/MSFT: 强趋势 (RankIC +0.18/+0.31), 高 ROC 应该**继续持有**
  - NVDA/TSLA: 反向模式 (asym<0), 信号相反
  - 腾讯/阿里 (HK): 尚未系统分析, 观察模式
"""

import argparse
import sys
import numpy as np
import pandas as pd
from datetime import datetime

from fetch_futu import realtime_quotes, get_kline, health_check, get_position_codes
from fractal_survey import mfdfa_spectrum


WATCHLISTS = {
    "us_tech": ["US.AAPL", "US.MSFT", "US.GOOGL", "US.NVDA", "US.TSLA", "US.META", "US.AMZN"],
    "us_energy_reverse": ["US.CVX", "US.XOM", "US.VLO", "US.PSX"],
    "hk_internet": ["HK.00700", "HK.09988", "HK.03690", "HK.01810"],
    "default": ["US.AAPL", "US.NVDA", "US.TSLA", "HK.00700", "HK.09988"],
    "positions": None,  # 动态从账户持仓读取
}


def fmt_pct(x, width=7):
    if pd.isna(x):
        return "-".rjust(width)
    sign = "+" if x >= 0 else ""
    return f"{sign}{x:.2f}%".rjust(width)


def classify_fractal(asym: float, hq2: float) -> str:
    """根据 MF-DFA 谱特征给出分形标签."""
    if pd.isna(asym) or pd.isna(hq2):
        return "?"
    tags = []
    # 分形非对称
    if asym > 0.3:
        tags.append("强非对称")
    elif asym > 0.1:
        tags.append("显著非对称")
    elif asym < -0.1:
        tags.append("反向⚠")
    else:
        tags.append("弱/中性")
    # 持续性
    if hq2 > 0.55:
        tags.append("持续")
    elif hq2 < 0.45:
        tags.append("反持续")
    return " · ".join(tags)


def analyze_one(code: str, live_row: pd.Series) -> dict:
    """对单只股票做实盘观察: 实时行情 + MF-DFA + 简单技术."""
    out = {
        "code": code,
        "last_price": live_row["last_price"],
        "chg_pct": live_row["change_rate"],
        "volume": live_row["volume"],
    }
    # 拉 200 天 K 线
    try:
        kl = get_kline(code, days=200, ktype="K_DAY")
        closes = kl["close"].astype(float)
        log_ret = np.log(closes / closes.shift(1))
        # MF-DFA 谱 (最新 120 日窗口)
        if len(log_ret) >= 120:
            spec = mfdfa_spectrum(log_ret.iloc[-120:])
            out.update({
                "delta_alpha": spec.get("delta_alpha"),
                "alpha0": spec.get("alpha0"),
                "hq2": spec.get("hq2"),
                "asym": spec.get("asym"),
            })
        # 简单技术指标
        ma20 = closes.rolling(20).mean().iloc[-1]
        ma60 = closes.rolling(60).mean().iloc[-1] if len(closes) >= 60 else np.nan
        out["ma20_diff"] = (closes.iloc[-1] / ma20 - 1) * 100 if pd.notna(ma20) else np.nan
        out["ma60_diff"] = (closes.iloc[-1] / ma60 - 1) * 100 if pd.notna(ma60) else np.nan
        # 20 日波动率年化
        out["vol_20d"] = log_ret.iloc[-20:].std() * np.sqrt(252) * 100
        # RSI6
        delta = closes.diff()
        gain = delta.where(delta > 0, 0).rolling(6).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(6).mean()
        rs = gain / loss
        out["rsi6"] = (100 - 100 / (1 + rs)).iloc[-1]
    except Exception as e:
        out["error"] = str(e)[:40]
    return out


def run(watchlist: list[str]) -> pd.DataFrame:
    """对关注池执行实盘观察."""
    # 健康检查
    hc = health_check()
    if not hc.get("qot_logined"):
        raise RuntimeError("Futu OpenD 行情未登录, 请先在 GUI 登录")
    us_state = hc.get("market_us")
    hk_state = hc.get("market_hk")
    sh_state = hc.get("market_sh")
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n{'═' * 88}")
    print(f"  实盘观察  {ts}   US:{us_state}  HK:{hk_state}  CN:{sh_state}")
    print("═" * 88)

    # 实时行情
    print(f"[1/2] 拉取实时行情 n={len(watchlist)}")
    rt = realtime_quotes(watchlist)
    rt = rt.set_index("code")

    # 逐只算分形 + 技术
    print(f"[2/2] 计算分形谱 + 技术指标")
    rows = []
    for code in watchlist:
        if code not in rt.index:
            print(f"  {code}: 实时行情缺失, 跳过")
            continue
        r = analyze_one(code, rt.loc[code])
        rows.append(r)

    df = pd.DataFrame(rows)
    return df


def format_report(df: pd.DataFrame) -> None:
    """打印格式化观察报告."""
    if df.empty:
        print("  无数据")
        return

    # 添加分形分类
    df["signal"] = df.apply(
        lambda r: classify_fractal(r.get("asym"), r.get("hq2")) if "error" not in r or pd.isna(r.get("error")) else "数据错",
        axis=1
    )

    print()
    cols = ["code", "last_price", "chg_pct", "rsi6", "ma20_diff", "vol_20d", "asym", "hq2", "signal"]
    header = f"  {'代码':<12} {'现价':>10} {'涨跌':>8} {'RSI6':>6} {'MA20%':>7} {'年化σ':>7} {'asym':>7} {'h(q=2)':>7}  {'分形标签'}"
    print(header)
    print("  " + "-" * 88)
    for _, r in df.iterrows():
        if pd.isna(r.get("asym")):
            extra = f"  {r.get('error', '数据不足')[:30]}"
            print(f"  {r['code']:<12} {r.get('last_price', 0):>10.2f} {fmt_pct(r.get('chg_pct')):>8}  {extra}")
            continue
        line = (
            f"  {r['code']:<12} "
            f"{r['last_price']:>10.2f} "
            f"{fmt_pct(r['chg_pct']):>8} "
            f"{r.get('rsi6', 0):>6.1f} "
            f"{fmt_pct(r.get('ma20_diff')):>7} "
            f"{r.get('vol_20d', 0):>6.1f}% "
            f"{r['asym']:>+7.3f} "
            f"{r['hq2']:>+7.3f}  "
            f"{r['signal']}"
        )
        print(line)

    # 汇总分形特征
    print()
    if "asym" in df.columns and df["asym"].notna().any():
        ap = (df["asym"] > 0).sum()
        n = df["asym"].notna().sum()
        print(f"  样本 asym>0: {ap}/{n}  |  asym 均值 {df['asym'].mean():+.3f}  |  h(q=2) 均值 {df['hq2'].mean():.3f}")
    print()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read Futu OpenD quotes and print live fractal observations")
    parser.add_argument("--watchlist", default="default", choices=sorted(WATCHLISTS))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    wl_name = args.watchlist

    print(f"使用关注池: {wl_name}")
    codes = WATCHLISTS[wl_name]
    if codes is None:  # 'positions' 模式
        codes = get_position_codes()
        if not codes:
            print("  账户无持仓或未解锁交易")
            sys.exit(0)
        print(f"  从账户读取持仓: {codes}")
    df = run(codes)
    format_report(df)

    out_path = f"futu_live_{wl_name}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    df.to_csv(out_path, index=False)
    print(f"  保存: {out_path}\n")


if __name__ == "__main__":
    main()
