"""信号命中率统计 — 读历史推荐 × 实际走势 → 算真实胜率

流程:
  1. 解析 advisor_history.log (launchd 累积的推荐记录)
  2. 对每条历史信号, 用 yfinance 查后续 N 天实际价格
  3. 判定信号方向是否正确
  4. 按策略/情境/股票分组汇总胜率

输出: 信号命中率报告 + CSV
"""

import re
import sys
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path


LOG_PATH = Path(__file__).parent / "advisor_history.log"


def parse_advisor_log(path: Path) -> pd.DataFrame:
    """解析 advisor_history.log, 抽出每条信号."""
    if not path.exists():
        return pd.DataFrame()

    text = path.read_text(encoding="utf-8")
    # 按运行时间分块
    blocks = re.split(r"═{70}\s*\n\s*运行时间:\s*([^\n]+)\s*\n═{70}", text)
    # blocks[0] 是第一块之前的内容, blocks[1] 是第一个时间, blocks[2] 是第一块内容...

    rows = []
    for i in range(1, len(blocks), 2):
        ts_raw = blocks[i].strip()
        content = blocks[i + 1] if i + 1 < len(blocks) else ""
        try:
            ts = pd.to_datetime(ts_raw.split(" CEST")[0].split(" EDT")[0])
        except Exception:
            continue

        # 解析每只股票条目
        for m in re.finditer(
            r"US\.(\w+)\s+现价=\$(\d+\.\d+)\s+今日\s+[+-]?\d+\.\d+%\s*\n"
            r"\s+分形:\s+asym=(?P<asym>[+-]?\d+\.\d+)\s+h\(q=2\)=(?P<hq2>[+-]?\d+\.\d+)\s+Δα=(?P<da>\d+\.\d+)\s*\n"
            r"\s+技术:\s+RSI6=(?P<rsi>\d+\.\d+)\s+MA20偏离=(?P<ma>[+-]?\d+\.\d+)%\s+年化σ=(?P<vol>\d+\.\d+)%\s*\n"
            r"\s+情境:\s+(?P<regime>\S+)\s*\n"
            r"\s+建议:\s+(?P<strategy>\S+)\s+到期天数目标=(?P<hold>\d+)",
            content,
        ):
            rows.append({
                "timestamp": ts,
                "symbol": m.group(1),
                "spot_at_signal": float(m.group(2)),
                "asym": float(m.group("asym")),
                "hq2": float(m.group("hq2")),
                "delta_alpha": float(m.group("da")),
                "rsi6": float(m.group("rsi")),
                "ma20_diff": float(m.group("ma")),
                "regime": m.group("regime"),
                "strategy": m.group("strategy"),
                "hold_days": int(m.group("hold")),
            })
    return pd.DataFrame(rows)


def check_outcome(row: pd.Series) -> dict:
    """用 yfinance 查信号后实际走势."""
    import yfinance as yf
    symbol = row["symbol"]
    signal_date = row["timestamp"]
    hold = row["hold_days"]
    end_date = signal_date + timedelta(days=hold + 3)
    start_date = signal_date - timedelta(days=2)

    try:
        t = yf.Ticker(symbol)
        hist = t.history(start=start_date.strftime("%Y-%m-%d"),
                         end=end_date.strftime("%Y-%m-%d"),
                         interval="1d")
        if hist.index.tz is not None:
            hist.index = hist.index.tz_localize(None)
        if hist.empty:
            return {}
        # 找离 signal_date 最近的收盘价
        hist = hist.sort_index()
        closes = hist["Close"].astype(float)

        # 信号日收盘 (或第一个可用日)
        signal_closes = closes[closes.index >= signal_date.normalize()]
        if signal_closes.empty:
            return {}
        close_at_signal = float(signal_closes.iloc[0])

        # 持有 N 天后
        target_date = signal_date + timedelta(days=hold)
        fwd_closes = closes[closes.index >= target_date.normalize()]
        if fwd_closes.empty:
            return {"completed": False}
        close_at_end = float(fwd_closes.iloc[0])

        fwd_ret_pct = (close_at_end / close_at_signal - 1) * 100
        if row["strategy"] == "BUY_PUT":
            correct = fwd_ret_pct < 0
        elif row["strategy"] == "BUY_CALL":
            correct = fwd_ret_pct > 0
        elif row["strategy"] == "BUY_STRADDLE":
            # 跨式: 需要 |fwd_ret| > ~7% (经验阈值, 近似盈亏平衡)
            correct = abs(fwd_ret_pct) > 7
        else:
            correct = False

        return {
            "completed": True,
            "close_at_signal": close_at_signal,
            "close_at_end": close_at_end,
            "fwd_ret_pct": fwd_ret_pct,
            "direction_correct": correct,
        }
    except Exception as e:
        return {"error": str(e)[:50]}


def report(df: pd.DataFrame) -> None:
    if df.empty:
        print("无信号历史.")
        return

    print(f"\n{'═' * 80}")
    print(f"  信号命中率报告 (历史信号 n={len(df)})")
    print("═" * 80)

    completed = df[df.get("completed", False).fillna(False)]
    if completed.empty:
        print("\n  所有信号持有期未结束, 等实际数据到位后重跑")
        return

    print(f"\n  已完成持有期的信号: {len(completed)} / {len(df)}")

    # 整体
    total = len(completed)
    wins = completed["direction_correct"].sum()
    print(f"  整体胜率: {wins}/{total} = {wins/total*100:.1f}%")
    print(f"  平均后续收益: {completed['fwd_ret_pct'].mean():+.2f}%")

    # 按策略
    print("\n  [按策略]")
    by_strat = completed.groupby("strategy").agg(
        n=("fwd_ret_pct", "size"),
        win_rate=("direction_correct", lambda x: x.mean() * 100),
        mean_ret=("fwd_ret_pct", "mean"),
    )
    print(by_strat.to_string(float_format=lambda x: f"{x:+.2f}"))

    # 按股票
    print("\n  [按股票]")
    by_sym = completed.groupby("symbol").agg(
        n=("fwd_ret_pct", "size"),
        win_rate=("direction_correct", lambda x: x.mean() * 100),
        mean_ret=("fwd_ret_pct", "mean"),
    )
    print(by_sym.sort_values("win_rate", ascending=False).to_string(float_format=lambda x: f"{x:+.2f}"))

    # 按情境
    print("\n  [按情境]")
    by_reg = completed.groupby("regime").agg(
        n=("fwd_ret_pct", "size"),
        win_rate=("direction_correct", lambda x: x.mean() * 100),
        mean_ret=("fwd_ret_pct", "mean"),
    )
    print(by_reg.sort_values("win_rate", ascending=False).to_string(float_format=lambda x: f"{x:+.2f}"))


def run() -> None:
    print(f"[1/3] 解析 {LOG_PATH}...")
    signals = parse_advisor_log(LOG_PATH)
    if signals.empty:
        print("  日志为空或解析失败. 运行 ./run_advisor_daily.sh 累积数据")
        return
    print(f"  抽出 {len(signals)} 条信号")

    print(f"\n[2/3] 查实际走势 (yfinance)...")
    outcomes = []
    for idx, row in signals.iterrows():
        o = check_outcome(row)
        outcomes.append(o)
    outcome_df = pd.DataFrame(outcomes)
    combined = pd.concat([signals, outcome_df], axis=1)

    print(f"[3/3] 汇总报告")
    report(combined)

    out = f"signal_hit_rate_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    combined.to_csv(out, index=False)
    print(f"\n  详细数据: {out}\n")


if __name__ == "__main__":
    run()
