"""IV 历史分位 (IV Rank) 模块

IV Rank 定义:
  IV Rank = (过去 N 日内 IV 低于当前的天数) / N × 100

解读:
  IV Rank < 25%: 当前 IV 历史低位, 期权便宜 → 适合买方 (买 Call/Put/跨式)
  IV Rank > 75%: 当前 IV 历史高位, 期权贵 → 适合卖方 (卖 Call/Put/跨式)
  25-75%: 中性

富途 API 没有历史 IV 数据. 当前双方案:
  1. realized_vol_rank: 用过去 252 天 20日实际波动率的分位作代理 (立即可用)
  2. iv_history.csv: 每次 option_monitor 跑时记录当前 IV, 积累 N 天后用真实 IV Rank
"""

import csv
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


HISTORY_PATH = Path(__file__).parent / "iv_history.csv"


def compute_realized_vol_rank(closes: pd.Series, current_realized_vol: float,
                              lookback_days: int = 252, window: int = 20) -> float | None:
    """基于过去 N 日 20日滚动实际波动率, 计算当前 realized vol 的分位.

    返回 0-100, 或 None (数据不足).
    """
    if len(closes) < lookback_days + window:
        return None
    log_ret = np.log(closes / closes.shift(1))
    rolling_vol = log_ret.rolling(window).std() * np.sqrt(252) * 100
    history = rolling_vol.dropna().tail(lookback_days)
    if len(history) < 30:
        return None
    rank = (history < current_realized_vol).mean() * 100
    return float(rank)


def log_iv(code: str, underlying: str, iv: float, expiry: str, strike: float) -> None:
    """追加一条 IV 记录到 CSV, 用于积累历史."""
    new_file = not HISTORY_PATH.exists()
    with HISTORY_PATH.open("a", newline="") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["timestamp", "code", "underlying", "iv", "expiry", "strike"])
        w.writerow([
            datetime.now().isoformat(timespec="seconds"),
            code, underlying, iv, expiry, strike,
        ])


def compute_iv_rank_from_history(underlying: str, current_iv: float,
                                  lookback_days: int = 252) -> float | None:
    """从本地 CSV 累积的历史 IV 计算 IV Rank. 252 天内未积累够不返回."""
    if not HISTORY_PATH.exists():
        return None
    try:
        df = pd.read_csv(HISTORY_PATH)
        df = df[df["underlying"] == underlying]
        if df.empty:
            return None
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        cutoff = datetime.now() - timedelta(days=lookback_days)
        df = df[df["timestamp"] >= cutoff]
        if len(df) < 30:
            return None  # 样本不足
        rank = (df["iv"] < current_iv).mean() * 100
        return float(rank)
    except Exception:
        return None


def get_iv_rank_best_effort(underlying: str, current_iv: float,
                             closes: pd.Series, current_realized_vol: float) -> tuple[float | None, str]:
    """优先用真实 IV 历史, 回落到 realized vol 代理.

    返回 (rank, source): source ∈ {"iv_history", "realized_vol_proxy", "insufficient"}
    """
    rank = compute_iv_rank_from_history(underlying, current_iv)
    if rank is not None:
        return rank, "iv_history"

    rank = compute_realized_vol_rank(closes, current_realized_vol)
    if rank is not None:
        return rank, "realized_vol_proxy"

    return None, "insufficient"


def describe_rank(rank: float | None, source: str) -> str:
    """把 IV Rank 数字转成文字描述."""
    if rank is None:
        return "历史数据不足"
    src_note = " (代理)" if source == "realized_vol_proxy" else ""
    if rank < 25:
        return f"低位 {rank:.0f}% · 期权便宜{src_note}"
    if rank > 75:
        return f"高位 {rank:.0f}% · 期权贵{src_note}"
    return f"中位 {rank:.0f}%{src_note}"
