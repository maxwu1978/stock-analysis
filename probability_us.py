"""美股专用趋势概率模型

与A股模型的关键差异:
1. 不使用财报因子 (yfinance季度数据太稀疏, IC=0)
2. 只用技术因子, 依赖滚动IC自适应每只股票的动量/反转特性
3. IC窗口缩短到90天 (美股波动更快, 120天太滞后)
4. 增加波动率因子 (ATR, 近期波动率变化)
"""

import pandas as pd
import numpy as np

# 美股专用因子 — 去掉所有财报因子
US_FACTOR_COLS = [
    "RSI6", "RSI12",
    "DIF", "DEA", "MACD",
    "ADX", "+DI", "-DI",
    "ROC5", "ROC10", "ROC20",
    "autocorr",
    "vol_price_div",
    "price_position",
    "ma5_slope",
    "ma20_diff",
    "ma60_diff",
    # 美股专用新增
    "atr_pct",        # ATR占比 (波动率)
    "vol_change",     # 成交量变化率
    "high_low_range", # 日内振幅
    "high52w_pos",    # 52周位置 (年度高低位置)
    # 肥尾前兆因子
    "boll_width",       # 布林带宽度
    "vol_compress",     # 短期/长期波动比
    "vol_surge",        # 量能异动
    "adx_accel",        # ADX加速度
    "kurt_20",          # 20日峰度
]

IC_WINDOW = 90   # 美股用更短窗口
HORIZON = 5


def _prepare_us_factors(df: pd.DataFrame) -> pd.DataFrame:
    """计算美股专用衍生因子"""
    # 标准技术因子衍生
    boll_spread = df.get("BOLL_UP", pd.Series(dtype=float)) - df.get("BOLL_DN", pd.Series(dtype=float))
    if "BOLL_DN" in df.columns:
        df["BOLL_pos"] = (df["close"] - df["BOLL_DN"]) / boll_spread.replace(0, np.nan) * 100

    if "VOL_MA5" in df.columns:
        df["vol_ratio"] = df["volume"] / df["VOL_MA5"].replace(0, np.nan)

    if "MA5" in df.columns:
        df["ma5_slope"] = df["MA5"].pct_change(3) * 100
    if "MA20" in df.columns:
        df["ma20_diff"] = (df["close"] / df["MA20"] - 1) * 100
    if "MA60" in df.columns:
        df["ma60_diff"] = (df["close"] / df["MA60"] - 1) * 100

    # 美股新增因子
    # ATR占价格的百分比 (波动率指标)
    tr1 = df["high"] - df["low"]
    tr2 = (df["high"] - df["close"].shift()).abs()
    tr3 = (df["low"] - df["close"].shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr14 = tr.rolling(14).mean()
    df["atr_pct"] = atr14 / df["close"] * 100

    # 成交量变化率 (5日均量 vs 20日均量)
    vol_ma5 = df["volume"].rolling(5).mean()
    vol_ma20 = df["volume"].rolling(20).mean()
    df["vol_change"] = (vol_ma5 / vol_ma20.replace(0, np.nan) - 1) * 100

    # 日内振幅
    df["high_low_range"] = (df["high"] - df["low"]) / df["close"] * 100

    # 52周位置 (George & Hwang 2004)
    # 当前价格在过去252个交易日高低区间的位置 (0~100)
    high_252 = df["high"].rolling(252).max()
    low_252 = df["low"].rolling(252).min()
    range_252 = (high_252 - low_252).replace(0, np.nan)
    df["high52w_pos"] = (df["close"] - low_252) / range_252 * 100

    return df


def _compute_rolling_ic(df, factor_cols, window=IC_WINDOW, horizon=HORIZON):
    fwd = df["close"].shift(-horizon) / df["close"] - 1
    df = df.copy()
    df["_fwd"] = fwd

    ic_df = pd.DataFrame(index=df.index)
    for col in factor_cols:
        if col not in df.columns:
            continue
        ic_df[col] = df[col].rolling(window).corr(df["_fwd"])
    return ic_df


def score_trend_us(df: pd.DataFrame) -> dict:
    """美股专用评分"""
    if len(df) < IC_WINDOW + HORIZON + 20:
        return {"error": f"数据不足{IC_WINDOW + HORIZON + 20}天"}

    df = _prepare_us_factors(df)

    # 过滤NaN过多的因子
    usable = [c for c in US_FACTOR_COLS if c in df.columns and df[c].notna().mean() > 0.3]
    if not usable:
        return {"error": "可用因子不足"}

    ic_df = _compute_rolling_ic(df, usable)

    last = df.iloc[-1]
    ic_row_idx = -HORIZON - 1
    if abs(ic_row_idx) >= len(ic_df):
        ic_row_idx = -1

    ic_weights = {}
    for col in usable:
        if col in ic_df.columns:
            v = ic_df[col].iloc[ic_row_idx]
            if pd.notna(v):
                ic_weights[col] = v

    if not ic_weights:
        return {"error": "无法计算IC权重"}

    # z-score标准化 + IC加权
    recent = df.iloc[-(IC_WINDOW + HORIZON):-HORIZON] if len(df) > IC_WINDOW + HORIZON else df
    factor_scores = {}
    for col, ic_w in ic_weights.items():
        val = last.get(col)
        if pd.isna(val):
            continue
        mean = recent[col].mean()
        std = recent[col].std()
        if pd.isna(std) or std == 0:
            continue
        z = (val - mean) / std
        contribution = z * ic_w * 100
        factor_scores[col] = {"value": val, "z_score": z, "ic_weight": ic_w, "contribution": contribution}

    raw_score = sum(f["contribution"] for f in factor_scores.values())
    n_factors = len(factor_scores)
    if n_factors > 0:
        max_theoretical = n_factors * 3 * 0.10 * 100
        score = raw_score / max_theoretical * 100 if max_theoretical > 0 else 0
        score = max(-100, min(100, score))
    else:
        score = 0

    if score >= 25:
        direction = "偏多"
    elif score >= 8:
        direction = "中性偏多"
    elif score <= -25:
        direction = "偏空"
    elif score <= -8:
        direction = "中性偏空"
    else:
        direction = "中性"

    # Regime
    adx = last.get("ADX", 25)
    if pd.isna(adx): adx = 25
    autocorr = last.get("autocorr", 0)
    if pd.isna(autocorr): autocorr = 0

    regime = {
        "stock_type": "momentum" if autocorr > 0.1 else ("mean_revert" if autocorr < -0.1 else "mixed"),
        "adx": adx,
        "trend_strength": "strong" if adx > 30 else ("moderate" if adx > 20 else "weak"),
        "autocorr": autocorr,
    }

    # 历史概率
    hist_prob = _compute_historical_probability_us(df, score, factor_scores, usable)

    return {
        "score": score,
        "direction": direction,
        "regime": regime,
        "historical_prob": hist_prob,
        "n_factors": n_factors,
    }


def _compute_historical_probability_us(df, current_score, factor_scores, usable_factors):
    if len(df) < IC_WINDOW + 50:
        return {}

    df = _prepare_us_factors(df)
    recent_window = df.iloc[IC_WINDOW:]
    if len(recent_window) < 50:
        return {}

    means = {}
    stds = {}
    for col in factor_scores:
        if col in df.columns:
            means[col] = recent_window[col].mean()
            stds[col] = recent_window[col].std()

    hist_scores = pd.Series(0.0, index=recent_window.index)
    for col, info in factor_scores.items():
        if col not in recent_window.columns:
            continue
        ic_w = info["ic_weight"]
        mean = means.get(col, 0)
        std = stds.get(col, 1)
        if std == 0:
            continue
        z = (recent_window[col] - mean) / std
        hist_scores += z * ic_w * 100

    n_factors = len(factor_scores)
    max_theoretical = n_factors * 3 * 0.10 * 100 if n_factors > 0 else 1
    if max_theoretical > 0:
        hist_scores = (hist_scores / max_theoretical * 100).clip(-100, 100)

    results = {}
    for horizon in [5, 10, 30, 180]:
        fwd = df["close"].shift(-horizon) / df["close"] - 1
        valid_idx = hist_scores.dropna().index.intersection(fwd.dropna().index)
        if len(valid_idx) < 30:
            continue

        hs = hist_scores[valid_idx]
        fw = fwd[valid_idx]
        score_std = hs.std()
        if score_std == 0:
            continue

        # 极端评分时用百分位匹配 (从最接近的top-N找样本)
        # 逐步放宽: 0.5std -> 1std -> 1.5std -> 最接近的20%样本
        similar_fwd = None
        for mult in [0.5, 1.0, 1.5, 2.0]:
            mask = (hs >= current_score - score_std * mult) & (hs <= current_score + score_std * mult)
            candidate = fw[mask]
            if len(candidate) >= 10:
                similar_fwd = candidate
                break

        if similar_fwd is None or len(similar_fwd) < 10:
            # 最后兜底: 取距离当前评分最近的20%历史样本
            distances = (hs - current_score).abs()
            top_n = max(int(len(hs) * 0.2), 30)
            closest_idx = distances.nsmallest(top_n).index
            similar_fwd = fw.loc[closest_idx]

        if len(similar_fwd) < 5:
            continue

        up_count = (similar_fwd > 0).sum()
        total = len(similar_fwd)

        results[f"{horizon}日"] = {
            "样本数": total,
            "上涨概率": f"{up_count / total * 100:.0f}%",
            "平均收益": f"{similar_fwd.mean() * 100:+.2f}%",
            "中位收益": f"{similar_fwd.median() * 100:+.2f}%",
            "波动率": f"{similar_fwd.std() * 100:.2f}%",
        }

    return results
