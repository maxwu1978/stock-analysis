"""技术指标计算模块"""

import pandas as pd
import numpy as np


def add_ma(df: pd.DataFrame, periods: list[int] = [5, 10, 20, 60]) -> pd.DataFrame:
    """添加移动平均线"""
    for p in periods:
        if len(df) >= p:
            df[f"MA{p}"] = df["close"].rolling(window=p).mean()
    return df


def add_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    """计算 MACD (DIF, DEA, MACD柱)"""
    ema_fast = df["close"].ewm(span=fast, adjust=False).mean()
    ema_slow = df["close"].ewm(span=slow, adjust=False).mean()
    df["DIF"] = ema_fast - ema_slow
    df["DEA"] = df["DIF"].ewm(span=signal, adjust=False).mean()
    df["MACD"] = 2 * (df["DIF"] - df["DEA"])
    return df


def add_rsi(df: pd.DataFrame, periods: list[int] = [6, 12, 24]) -> pd.DataFrame:
    """计算 RSI"""
    delta = df["close"].diff()
    for p in periods:
        gain = delta.where(delta > 0, 0.0).rolling(window=p).mean()
        loss = (-delta.where(delta < 0, 0.0)).rolling(window=p).mean()
        rs = gain / loss.replace(0, np.nan)
        df[f"RSI{p}"] = 100 - (100 / (1 + rs))
    return df


def add_boll(df: pd.DataFrame, period: int = 20, std_dev: int = 2) -> pd.DataFrame:
    """计算布林带"""
    df["BOLL_MID"] = df["close"].rolling(window=period).mean()
    rolling_std = df["close"].rolling(window=period).std()
    df["BOLL_UP"] = df["BOLL_MID"] + std_dev * rolling_std
    df["BOLL_DN"] = df["BOLL_MID"] - std_dev * rolling_std
    return df


def add_volume_ma(df: pd.DataFrame, period: int = 5) -> pd.DataFrame:
    """计算成交量均线"""
    df[f"VOL_MA{period}"] = df["volume"].rolling(window=period).mean()
    return df


def add_adx(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """计算 ADX (Average Directional Index) 趋势强度指标"""
    high = df["high"]
    low = df["low"]
    close = df["close"]

    plus_dm = high.diff()
    minus_dm = -low.diff()
    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0.0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)

    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    atr = tr.ewm(span=period, adjust=False).mean()
    plus_di = 100 * (plus_dm.ewm(span=period, adjust=False).mean() / atr)
    minus_di = 100 * (minus_dm.ewm(span=period, adjust=False).mean() / atr)
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    df["ADX"] = dx.ewm(span=period, adjust=False).mean()
    df["+DI"] = plus_di
    df["-DI"] = minus_di
    return df


def add_roc(df: pd.DataFrame, periods: list[int] = [5, 10, 20]) -> pd.DataFrame:
    """计算 ROC (Rate of Change) 动量指标"""
    for p in periods:
        if len(df) >= p:
            df[f"ROC{p}"] = (df["close"] / df["close"].shift(p) - 1) * 100
    return df


def add_vol_price_divergence(df: pd.DataFrame, period: int = 10) -> pd.DataFrame:
    """检测量价背离：价格与成交量趋势方向不一致"""
    if len(df) < period:
        return df
    price_chg = df["close"].rolling(period).apply(
        lambda x: np.polyfit(range(len(x)), x, 1)[0] if len(x) == period else 0, raw=True)
    vol_chg = df["volume"].rolling(period).apply(
        lambda x: np.polyfit(range(len(x)), x, 1)[0] if len(x) == period else 0, raw=True)
    # 标准化为方向
    df["price_trend"] = np.sign(price_chg)
    df["vol_trend"] = np.sign(vol_chg)
    # 背离 = 方向不一致 (-1: 价涨量缩 or 价跌量增)
    df["vol_price_div"] = -(df["price_trend"] * df["vol_trend"])
    return df


def add_autocorr(df: pd.DataFrame, period: int = 20) -> pd.DataFrame:
    """计算收益率自相关系数 — 判断股性(动量 vs 均值回归)
    正值 = 动量特征(涨了还涨), 负值 = 均值回归特征(涨了就跌)
    """
    ret = df["close"].pct_change()
    df["autocorr"] = ret.rolling(window=period).apply(
        lambda x: x.autocorr(lag=1) if len(x) == period else 0, raw=False)
    return df


def add_support_resistance(df: pd.DataFrame, period: int = 20) -> pd.DataFrame:
    """计算近期支撑位和阻力位"""
    df["resist"] = df["high"].rolling(window=period).max()
    df["support"] = df["low"].rolling(window=period).min()
    # 价格在区间中的相对位置 0~100
    spread = df["resist"] - df["support"]
    df["price_position"] = ((df["close"] - df["support"]) / spread.replace(0, np.nan) * 100)
    return df


def add_high52w_pos(df: pd.DataFrame, period: int = 250) -> pd.DataFrame:
    """计算年度高低位置因子 (52-week high/low position)
    value = (close - 250日低点) / (250日高点 - 250日低点), 范围0~100
    学术依据: George & Hwang (2004) 52-week high与动量效应
    靠近年高 → 动量延续; 靠近年低 → 可能反转或继续下跌
    """
    high_252 = df["high"].rolling(window=period, min_periods=period // 2).max()
    low_252 = df["low"].rolling(window=period, min_periods=period // 2).min()
    spread = high_252 - low_252
    df["high52w_pos"] = ((df["close"] - low_252) / spread.replace(0, np.nan) * 100)
    return df


def compute_all(df: pd.DataFrame, fundamental_df: pd.DataFrame = None) -> pd.DataFrame:
    """计算所有技术指标, 可选整合基本面因子"""
    df = add_ma(df)
    df = add_macd(df)
    df = add_rsi(df)
    df = add_boll(df)
    df = add_volume_ma(df)
    df = add_adx(df)
    df = add_roc(df)
    df = add_vol_price_divergence(df)
    df = add_autocorr(df)
    df = add_support_resistance(df)
    df = add_high52w_pos(df)

    # 整合基本面因子
    if fundamental_df is not None and not fundamental_df.empty:
        from fundamental import align_fundamental_to_daily
        df = align_fundamental_to_daily(fundamental_df, df)

    return df


def summarize(df: pd.DataFrame) -> dict:
    """提取最新一行的关键指标摘要"""
    if df.empty:
        return {}
    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else last

    summary = {
        "收盘价": f"{last['close']:.2f}",
        "MA5": f"{last.get('MA5', 0):.2f}" if pd.notna(last.get("MA5")) else "-",
        "MA10": f"{last.get('MA10', 0):.2f}" if pd.notna(last.get("MA10")) else "-",
        "MA20": f"{last.get('MA20', 0):.2f}" if pd.notna(last.get("MA20")) else "-",
        "MA60": f"{last.get('MA60', 0):.2f}" if pd.notna(last.get("MA60")) else "-",
        "DIF": f"{last.get('DIF', 0):.3f}" if pd.notna(last.get("DIF")) else "-",
        "DEA": f"{last.get('DEA', 0):.3f}" if pd.notna(last.get("DEA")) else "-",
        "MACD": f"{last.get('MACD', 0):.3f}" if pd.notna(last.get("MACD")) else "-",
        "RSI6": f"{last.get('RSI6', 0):.1f}" if pd.notna(last.get("RSI6")) else "-",
        "RSI12": f"{last.get('RSI12', 0):.1f}" if pd.notna(last.get("RSI12")) else "-",
        "BOLL上轨": f"{last.get('BOLL_UP', 0):.2f}" if pd.notna(last.get("BOLL_UP")) else "-",
        "BOLL中轨": f"{last.get('BOLL_MID', 0):.2f}" if pd.notna(last.get("BOLL_MID")) else "-",
        "BOLL下轨": f"{last.get('BOLL_DN', 0):.2f}" if pd.notna(last.get("BOLL_DN")) else "-",
    }

    # 客观信号标注
    signals = []
    # MACD 金叉/死叉
    if pd.notna(last.get("DIF")) and pd.notna(prev.get("DIF")):
        if prev["DIF"] <= prev["DEA"] and last["DIF"] > last["DEA"]:
            signals.append("MACD金叉")
        elif prev["DIF"] >= prev["DEA"] and last["DIF"] < last["DEA"]:
            signals.append("MACD死叉")

    # RSI 超买/超卖
    rsi6 = last.get("RSI6")
    if pd.notna(rsi6):
        if rsi6 > 80:
            signals.append(f"RSI6超买({rsi6:.0f})")
        elif rsi6 < 20:
            signals.append(f"RSI6超卖({rsi6:.0f})")

    # 价格与均线关系
    close = last["close"]
    ma5 = last.get("MA5")
    ma20 = last.get("MA20")
    if pd.notna(ma5) and pd.notna(ma20):
        if close > ma5 > ma20:
            signals.append("多头排列(价>MA5>MA20)")
        elif close < ma5 < ma20:
            signals.append("空头排列(价<MA5<MA20)")

    # 布林带位置
    boll_up = last.get("BOLL_UP")
    boll_dn = last.get("BOLL_DN")
    if pd.notna(boll_up) and pd.notna(boll_dn):
        if close >= boll_up:
            signals.append("触及布林上轨")
        elif close <= boll_dn:
            signals.append("触及布林下轨")

    summary["信号"] = " | ".join(signals) if signals else "无明显信号"
    return summary
