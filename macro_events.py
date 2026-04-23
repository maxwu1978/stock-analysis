"""宏观事件 + 财报日历 + VIX 风险监控

功能:
  1. VIX 实时水平 (yfinance ^VIX)
  2. 目标股未来 N 天财报日期 (yfinance earnings)
  3. FOMC 会议日程 (2026 年硬编码, 每年 2 月补充更新)
  4. CPI / NFP 经济数据公告日程 (硬编码)
  5. 综合风险警告 get_risk_warnings(symbol) 返回列表

用法:
  from macro_events import get_vix_level, get_risk_warnings, MACRO_CALENDAR
  vix = get_vix_level()
  warnings = get_risk_warnings('US.NVDA', days_ahead=14)
  # → ['⚠ 财报将于 5月2日公布 (7天内, 避免跨式)',
  #    '⚠ FOMC 4月29日 (8天内)',
  #    '⚠ VIX 高达 28, 期权买方成本高']
"""

from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path

import pandas as pd
import yfinance as yf


# ==================== 2026 年宏观日程 ====================
# 2026 年 FOMC 会议 (8 次, 美东时间, 决议公布次日下午 2 点)
FOMC_2026 = [
    "2026-01-29",
    "2026-03-19",
    "2026-04-30",
    "2026-06-18",
    "2026-07-30",
    "2026-09-17",
    "2026-10-29",
    "2026-12-10",
]

# 2026 年 CPI 发布 (每月第二周三左右, BLS 公告)
CPI_2026 = [
    "2026-01-13", "2026-02-11", "2026-03-11", "2026-04-10",
    "2026-05-13", "2026-06-10", "2026-07-15", "2026-08-12",
    "2026-09-11", "2026-10-15", "2026-11-13", "2026-12-10",
]

# 2026 年 NFP (非农) 每月第一个周五
NFP_2026 = [
    "2026-01-02", "2026-02-06", "2026-03-06", "2026-04-03",
    "2026-05-01", "2026-06-05", "2026-07-03", "2026-08-07",
    "2026-09-04", "2026-10-02", "2026-11-06", "2026-12-04",
]

MACRO_CALENDAR = {
    "FOMC": FOMC_2026,
    "CPI": CPI_2026,
    "NFP": NFP_2026,
}


@lru_cache(maxsize=4)
def get_vix_history(period: str = "10y") -> pd.DataFrame:
    """缓存 VIX 历史数据，供主模型对齐宏观波动因子。"""
    v = yf.Ticker("^VIX")
    hist = v.history(period=period, interval="1d", auto_adjust=False)
    if hist.index.tz is not None:
        hist.index = hist.index.tz_localize(None)
    out = hist[["Close"]].rename(columns={"Close": "vix_close"}).copy()
    out["vix_ma20"] = out["vix_close"].rolling(20).mean()
    out["vix_std20"] = out["vix_close"].rolling(20).std()
    out["vix_z20"] = (out["vix_close"] - out["vix_ma20"]) / out["vix_std20"].replace(0, pd.NA)
    out["vix_ma20_diff"] = (out["vix_close"] / out["vix_ma20"].replace(0, pd.NA) - 1) * 100
    out["vix_roc5"] = out["vix_close"].pct_change(5) * 100
    return out


def add_us_macro_factors(df: pd.DataFrame) -> pd.DataFrame:
    """为美股日线对齐可历史回测的宏观因子，目前只接入 VIX 系列。"""
    out = df.copy()
    try:
        vix = get_vix_history()
    except Exception:
        out["vix_close"] = pd.NA
        out["vix_z20"] = pd.NA
        out["vix_ma20_diff"] = pd.NA
        out["vix_roc5"] = pd.NA
        return out

    aligned = vix.reindex(out.index).ffill()
    for col in ["vix_close", "vix_z20", "vix_ma20_diff", "vix_roc5"]:
        out[col] = aligned[col]
    return out


def get_vix_level() -> dict:
    """拉 ^VIX 当前水平 + 20 日均值.
    返回: {current, ma20, percentile, level_desc}
    """
    try:
        v = yf.Ticker("^VIX")
        hist = v.history(period="60d", interval="1d", auto_adjust=False)
        if hist.index.tz is not None:
            hist.index = hist.index.tz_localize(None)
        closes = hist["Close"].astype(float)
        current = float(closes.iloc[-1])
        ma20 = float(closes.iloc[-20:].mean())
        # 20 日分位
        percentile = (closes.iloc[-20:] < current).mean() * 100

        # 级别描述
        if current < 15:
            desc = "极低"
        elif current < 20:
            desc = "低"
        elif current < 25:
            desc = "中性"
        elif current < 30:
            desc = "偏高"
        elif current < 40:
            desc = "高 (恐慌)"
        else:
            desc = "极高 (市场动荡)"

        return {
            "current": current,
            "ma20": ma20,
            "percentile_20d": percentile,
            "level_desc": desc,
        }
    except Exception as e:
        return {"error": str(e)[:50]}


def get_earnings_date(symbol: str) -> str | None:
    """查单只股票的下一次财报日期.
    symbol: 'NVDA' 或 'US.NVDA' (自动剥离前缀)
    返回 YYYY-MM-DD 或 None.
    """
    code = symbol.replace("US.", "").replace("HK.", "")
    try:
        t = yf.Ticker(code)
        # yfinance 的 earnings_dates 返回过去+未来
        ed = t.earnings_dates
        if ed is None or ed.empty:
            return None
        # 去时区
        if ed.index.tz is not None:
            ed.index = ed.index.tz_localize(None)
        now = datetime.now()
        future = ed[ed.index >= now]
        if future.empty:
            return None
        return future.index[0].strftime("%Y-%m-%d")
    except Exception:
        return None


def _parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")


def get_upcoming_macro_events(days_ahead: int = 30) -> list[dict]:
    """返回未来 N 天的宏观事件列表, 按日期升序."""
    now = datetime.now()
    cutoff = now + timedelta(days=days_ahead)
    events = []
    for event_type, dates in MACRO_CALENDAR.items():
        for d_str in dates:
            d = _parse_date(d_str)
            if now <= d <= cutoff:
                days_until = (d - now).days + 1
                events.append({
                    "type": event_type,
                    "date": d_str,
                    "days_until": days_until,
                })
    events.sort(key=lambda x: x["date"])
    return events


def get_risk_warnings(symbol: str | None = None, days_ahead: int = 14) -> list[str]:
    """综合风险警告列表.
    symbol: 'US.NVDA' 等, 检查特定股的财报风险. None 则只检查宏观+VIX.
    """
    warnings = []

    # VIX
    vix = get_vix_level()
    if "error" not in vix:
        if vix["current"] >= 30:
            warnings.append(f"🔴 VIX {vix['current']:.1f} ({vix['level_desc']}) — 市场恐慌, 避免期权买方")
        elif vix["current"] >= 25:
            warnings.append(f"🟡 VIX {vix['current']:.1f} ({vix['level_desc']}) — 期权 IV 偏贵")

    # 宏观事件
    events = get_upcoming_macro_events(days_ahead=days_ahead)
    for e in events:
        if e["days_until"] <= 3:
            warnings.append(f"🔴 {e['type']} {e['date']} (仅剩 {e['days_until']}天) — 高波动风险期")
        elif e["days_until"] <= 7:
            warnings.append(f"🟡 {e['type']} {e['date']} ({e['days_until']}天后) — 临近事件, 小心跨式")
        elif e["days_until"] <= days_ahead:
            warnings.append(f"⚪ {e['type']} {e['date']} ({e['days_until']}天后)")

    # 财报风险 (针对特定股)
    if symbol:
        er_date = get_earnings_date(symbol)
        if er_date:
            days = (_parse_date(er_date) - datetime.now()).days + 1
            if 0 < days <= 3:
                warnings.append(f"🔴 {symbol} 财报 {er_date} (仅剩 {days}天) — 禁止买跨式 (IV crush 风险)")
            elif days <= 7:
                warnings.append(f"🟡 {symbol} 财报 {er_date} ({days}天后) — IV 已上升, 跨式性价比低")
            elif days <= days_ahead:
                warnings.append(f"⚪ {symbol} 财报 {er_date} ({days}天后)")

    return warnings


if __name__ == "__main__":
    print("=" * 60)
    print("  宏观事件风险监控")
    print("=" * 60)
    print()

    # VIX
    vix = get_vix_level()
    if "error" in vix:
        print(f"VIX 查询失败: {vix['error']}")
    else:
        print(f"VIX 当前: {vix['current']:.2f}  ({vix['level_desc']})")
        print(f"  20日均值: {vix['ma20']:.2f}")
        print(f"  20日分位: {vix['percentile_20d']:.0f}%")
    print()

    # 未来 30 天宏观事件
    print("未来 30 天宏观事件:")
    events = get_upcoming_macro_events(30)
    for e in events:
        marker = "🔴" if e["days_until"] <= 3 else ("🟡" if e["days_until"] <= 7 else "⚪")
        print(f"  {marker} {e['type']:4s} {e['date']} ({e['days_until']:3d} 天后)")
    print()

    # 主要关注股的财报
    print("关注股未来 30 天财报:")
    for sym in ["US.NVDA", "US.AAPL", "US.MSFT", "US.GOOGL", "US.TSLA", "US.META", "US.AMZN", "US.FUTU"]:
        ed = get_earnings_date(sym)
        if ed:
            days = (_parse_date(ed) - datetime.now()).days + 1
            if days <= 30:
                marker = "🔴" if days <= 3 else ("🟡" if days <= 7 else "⚪")
                print(f"  {marker} {sym:<10s} {ed} ({days:3d} 天后)")
            else:
                print(f"     {sym:<10s} {ed} ({days}天后, >30d)")
        else:
            print(f"     {sym:<10s} (无公告财报日期)")
    print()

    # 综合警告示例 (NVDA)
    print("示例: NVDA 综合风险警告")
    for w in get_risk_warnings("US.NVDA", days_ahead=14):
        print(f"  {w}")
