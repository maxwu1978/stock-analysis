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
        # 处理边界情况:
        # - loss=0 (连续全涨): RSI=100
        # - gain=0 (连续全跌): RSI=0
        # - 其他: 正常计算
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        # 当loss=0时, rs=inf, RSI理论上=100
        rsi = rsi.where(loss != 0, 100.0)
        # 当gain=0且loss>0时, rs=0, RSI=0
        rsi = rsi.where(~((gain == 0) & (loss > 0)), 0.0)
        df[f"RSI{p}"] = rsi
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


def add_max_ret_20d(df: pd.DataFrame, period: int = 20) -> pd.DataFrame:
    """计算20日最大单日涨幅因子 (MAX factor)
    value = 过去20日中最大的单日收益率 (%)
    学术依据: Bali et al. (2011) MAX效应, 中国A股实证: 高MAX股票彩票偏好→过高估值→未来负超额收益
    参考: 'Factor MAX in the Chinese Market' (EFMA 2025), 'Dissecting the lottery-like anomaly in China' (2025)
    预期IC方向: 负 (高MAX→主力/散户追涨→溢价→后续回调)
    """
    daily_ret = df["close"].pct_change() * 100
    df["max_ret_20d"] = daily_ret.rolling(window=period, min_periods=period // 2).max()
    return df


def add_gap_ret_10d(df: pd.DataFrame, period: int = 10) -> pd.DataFrame:
    """计算隔夜跳空收益率因子 (Overnight Gap Return)
    gap_ret = (open_t - close_{t-1}) / close_{t-1} * 100，取10日滚动均值
    学术依据: 隔夜收益与日内收益由不同投资者主导（机构 vs 散户），A股隔夜高溢价预示均值回归
    参考: 新浪财经2025年隔夜与日间网络关系因子研究; 隔夜收益领先效应（Aboody et al.）
    预期IC方向: 负（持续隔夜高开→追涨情绪→短期回调）
    """
    gap_ret = (df["open"] - df["close"].shift(1)) / df["close"].shift(1) * 100
    df["gap_ret_10d"] = gap_ret.rolling(window=period, min_periods=period // 2).mean()
    return df


def add_amihud_20d(df: pd.DataFrame, period: int = 20) -> pd.DataFrame:
    """20日 Amihud 非流动性因子
    ILLIQ = mean(|daily_ret| / dollar_volume, 20d) * 1e8
    学术依据: Amihud (2002) 非流动性溢价; A股实证IC=8.78% (高频因子研究2024)
    时序含义: 近期价格对成交金额的敏感度高→散户追涨导致以小量撬动大价格→均值回归信号
    预期IC方向: 负 (高非流动性→散户主导→后续回调)
    """
    daily_ret = df["close"].pct_change().abs()
    dollar_vol = (df["close"] * df["volume"]).replace(0, np.nan)
    amihud_daily = daily_ret / dollar_vol * 1e8
    df["amihud_20d"] = amihud_daily.rolling(window=period, min_periods=period // 2).mean()
    return df


def add_fat_tail_signals(df: pd.DataFrame) -> pd.DataFrame:
    """肥尾前兆信号 — 基于实证研究的5个前兆因子"""
    # 1. 布林带宽度 (越宽=波动越大, 正肥尾前偏高)
    if "BOLL_UP" in df.columns and "BOLL_DN" in df.columns and "BOLL_MID" in df.columns:
        df["boll_width"] = (df["BOLL_UP"] - df["BOLL_DN"]) / df["BOLL_MID"].replace(0, np.nan) * 100

    # 2. 短期/长期波动率比 (短期波动收缩=正肥尾前兆)
    ret_vol_5 = df["close"].pct_change().rolling(5).std()
    ret_vol_20 = df["close"].pct_change().rolling(20).std()
    df["vol_compress"] = ret_vol_5 / ret_vol_20.replace(0, np.nan)

    # 3. 量能异动 (5日均量/20日均量, 正肥尾前偏高=资金进场)
    vol_ma5 = df["volume"].rolling(5).mean()
    vol_ma20 = df["volume"].rolling(20).mean()
    df["vol_surge"] = vol_ma5 / vol_ma20.replace(0, np.nan)

    # 4. ADX加速度 (ADX 5日变化, 正肥尾前偏高=趋势正在形成)
    if "ADX" in df.columns:
        df["adx_accel"] = df["ADX"].diff(5)

    # 5. 20日峰度 (越低=分布越平, 正肥尾前偏低=即将出现极端值)
    df["kurt_20"] = df["close"].pct_change().rolling(20).apply(
        lambda x: x.kurtosis() if len(x) == 20 else np.nan, raw=False)

    # 综合肥尾评分: 当多个前兆同时出现时标记
    # 条件: boll_width > 中位数 & RSI6 < 40 & vol_surge > 1.05
    if "RSI6" in df.columns and "boll_width" in df.columns:
        bw_median = df["boll_width"].rolling(120).median()
        cond_bw = df["boll_width"] > bw_median  # 波动放大
        cond_rsi = df["RSI6"] < 40               # 超跌
        cond_vol = df["vol_surge"] > 1.05         # 放量

        score = cond_bw.astype(int) + cond_rsi.astype(int) + cond_vol.astype(int)
        if "adx_accel" in df.columns:
            cond_adx = df["adx_accel"] > 1        # ADX加速
            score += cond_adx.astype(int)
        if "kurt_20" in df.columns:
            kurt_median = df["kurt_20"].rolling(120).median()
            cond_kurt = df["kurt_20"] < kurt_median  # 峰度偏低
            score += cond_kurt.astype(int)

        df["fat_tail_score"] = score  # 0~5分, 越高越可能出现正肥尾

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
    df = add_max_ret_20d(df)
    df = add_gap_ret_10d(df)
    df = add_amihud_20d(df)

    df = add_fat_tail_signals(df)

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
