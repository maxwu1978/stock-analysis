"""自适应趋势概率评估模型 v3 — 数据驱动

核心思路:
1. 用滚动窗口计算每个因子的历史IC (与未来5日收益的相关系数)
2. IC值直接作为因子权重和方向 (正IC=动量, 负IC=反转, 自动适应)
3. 最终评分 = Σ(标准化因子值 × IC权重)
4. 避免人为判断因子方向, 完全由数据决定
"""

import pandas as pd
import numpy as np

# 使用的因子 — 技术因子 + 基本面因子
FACTOR_COLS = [
    # 技术因子
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
    # 基本面因子
    "roe",              # ROE (净资产收益率)
    "rev_growth",       # 营收同比增长率
    "profit_growth",    # 净利润同比增长率
    "gross_margin",     # 毛利率
    "debt_ratio",       # 资产负债率
    "cash_flow_ps",     # 每股经营现金流
    "roe_chg",          # ROE环比变化
    "rev_growth_accel", # 营收加速度
]

# 滚动窗口长度 (用多少天数据估计IC)
IC_WINDOW = 120
# 预测周期
HORIZON = 5


def _compute_rolling_ic(df: pd.DataFrame, window: int = IC_WINDOW, horizon: int = HORIZON, factor_cols: list = None) -> pd.DataFrame:
    """计算每个因子的滚动IC"""
    # 未来收益率
    fwd = df["close"].shift(-horizon) / df["close"] - 1
    df = df.copy()
    df["_fwd"] = fwd

    if factor_cols is None:
        factor_cols = FACTOR_COLS
    ic_df = pd.DataFrame(index=df.index)
    for col in factor_cols:
        if col not in df.columns:
            continue
        # 滚动rank correlation
        ic_series = df[col].rolling(window).corr(df["_fwd"])
        ic_df[col] = ic_series

    return ic_df


def _prepare_extra_factors(df: pd.DataFrame) -> pd.DataFrame:
    """计算不在indicators.py中的衍生因子"""
    boll_spread = df["BOLL_UP"] - df["BOLL_DN"]
    df["BOLL_pos"] = (df["close"] - df["BOLL_DN"]) / boll_spread.replace(0, np.nan) * 100

    if "VOL_MA5" in df.columns:
        df["vol_ratio_factor"] = df["volume"] / df["VOL_MA5"].replace(0, np.nan)

    df["ma5_slope"] = df["MA5"].pct_change(3) * 100
    df["ma20_diff"] = (df["close"] / df["MA20"] - 1) * 100
    df["ma60_diff"] = (df["close"] / df["MA60"] - 1) * 100

    return df


def score_trend(df: pd.DataFrame) -> dict:
    """数据驱动评分: 用滚动IC作为因子权重"""
    if len(df) < IC_WINDOW + HORIZON + 20:
        return {"error": f"数据不足{IC_WINDOW + HORIZON + 20}天"}

    df = _prepare_extra_factors(df)

    # 过滤掉NaN超过50%的因子 (美股财报数据稀疏时自动跳过)
    usable_factors = []
    for col in FACTOR_COLS:
        if col in df.columns and df[col].notna().mean() > 0.3:
            usable_factors.append(col)
    if not usable_factors:
        return {"error": "可用因子不足"}

    # 计算滚动IC (只用可用因子)
    ic_df = _compute_rolling_ic(df, factor_cols=usable_factors)

    last_idx = df.index[-1]
    last = df.iloc[-1]

    # 取最近的IC值作为权重 (使用倒数第HORIZON+1行, 因为最后HORIZON行没有fwd)
    ic_row_idx = -HORIZON - 1
    if abs(ic_row_idx) >= len(ic_df):
        ic_row_idx = -1

    ic_weights = {}
    for col in usable_factors:
        if col in ic_df.columns:
            ic_val = ic_df[col].iloc[ic_row_idx]
            if pd.notna(ic_val):
                ic_weights[col] = ic_val

    if not ic_weights:
        return {"error": "无法计算IC权重"}

    # 对因子值做标准化 (z-score, 用最近120天)
    recent = df.iloc[-(IC_WINDOW + HORIZON):-HORIZON] if len(df) > IC_WINDOW + HORIZON else df
    factor_scores = {}
    for col, ic_w in ic_weights.items():
        if col not in df.columns:
            continue
        val = last.get(col)
        if pd.isna(val):
            continue
        mean = recent[col].mean()
        std = recent[col].std()
        if pd.isna(std) or std == 0:
            continue
        z = (val - mean) / std
        # 贡献 = z * IC权重 (IC为负时, 高z值贡献负分 -> 反转)
        contribution = z * ic_w * 100  # 放大到合理范围
        factor_scores[col] = {
            "value": val,
            "z_score": z,
            "ic_weight": ic_w,
            "contribution": contribution,
        }

    # 综合评分
    raw_score = sum(f["contribution"] for f in factor_scores.values())

    # 归一化到 -100 ~ +100
    n_factors = len(factor_scores)
    if n_factors > 0:
        # 理论最大值约为 n * 3 * 0.15 * 100 (z=3, IC=0.15)
        max_theoretical = n_factors * 3 * 0.10 * 100
        score = raw_score / max_theoretical * 100 if max_theoretical > 0 else 0
        score = max(-100, min(100, score))
    else:
        score = 0

    # 方向判断
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

    # 提取关键因素 (按贡献绝对值排序)
    sorted_factors = sorted(factor_scores.items(), key=lambda x: abs(x[1]["contribution"]), reverse=True)
    bullish = []
    bearish = []
    for col, info in sorted_factors[:8]:  # 只显示前8个
        contrib = info["contribution"]
        ic_w = info["ic_weight"]
        z = info["z_score"]
        val = info["value"]

        # 解释
        ic_dir = "动量" if ic_w > 0 else "反转"
        if abs(contrib) < 0.5:
            continue

        if isinstance(val, float):
            val_str = f"{val:.2f}"
        else:
            val_str = str(val)

        desc = f"{col}={val_str} (z={z:+.1f}, IC={ic_w:+.3f}/{ic_dir}, 贡献{contrib:+.1f})"
        if contrib > 0:
            bullish.append(desc)
        else:
            bearish.append(desc)

    # Regime信息
    regime = _classify_regime(df, ic_weights)

    # 历史统计
    hist_prob = _compute_historical_probability(df, score, factor_scores)

    return {
        "bullish_factors": bullish,
        "bearish_factors": bearish,
        "score": round(score),
        "direction": direction,
        "regime": regime,
        "historical_prob": hist_prob,
        "raw_score": raw_score,
        "n_factors": n_factors,
    }


def _classify_regime(df: pd.DataFrame, ic_weights: dict) -> dict:
    """从IC权重判断当前市场状态"""
    last = df.iloc[-1]

    # 看IC方向分布: 大多数因子IC为负 = 均值回归市场
    neg_count = sum(1 for v in ic_weights.values() if v < -0.02)
    pos_count = sum(1 for v in ic_weights.values() if v > 0.02)
    total = neg_count + pos_count
    if total > 0:
        revert_ratio = neg_count / total
    else:
        revert_ratio = 0.5

    if revert_ratio > 0.7:
        stock_type = "mean_revert"
    elif revert_ratio < 0.3:
        stock_type = "momentum"
    else:
        stock_type = "mixed"

    adx = last.get("ADX", 25)
    if pd.isna(adx):
        adx = 25
    trend_strength = "strong" if adx > 30 else ("moderate" if adx > 20 else "weak")

    plus_di = last.get("+DI", 25)
    minus_di = last.get("-DI", 25)
    trend_dir = "up" if (pd.notna(plus_di) and pd.notna(minus_di) and plus_di > minus_di) else "down"

    autocorr = last.get("autocorr", 0)
    if pd.isna(autocorr):
        autocorr = 0

    boll_up = last.get("BOLL_UP")
    boll_dn = last.get("BOLL_DN")
    boll_mid = last.get("BOLL_MID")
    if pd.notna(boll_up) and pd.notna(boll_dn) and pd.notna(boll_mid) and boll_mid > 0:
        boll_width = (boll_up - boll_dn) / boll_mid * 100
    else:
        boll_width = 10
    volatility = "high" if boll_width > 15 else ("normal" if boll_width > 8 else "low")

    # 最强IC因子
    top_ic = sorted(ic_weights.items(), key=lambda x: abs(x[1]), reverse=True)[:3]
    top_factors = ", ".join(f"{k}({v:+.3f})" for k, v in top_ic)

    return {
        "stock_type": stock_type,
        "autocorr": autocorr,
        "trend_strength": trend_strength,
        "adx": adx,
        "trend_dir": trend_dir,
        "volatility": volatility,
        "boll_width": boll_width,
        "revert_ratio": revert_ratio,
        "top_factors": top_factors,
    }


def _compute_historical_probability(df: pd.DataFrame, current_score: float, factor_scores: dict) -> dict:
    """基于合成评分的历史概率统计"""
    if len(df) < IC_WINDOW + 50:
        return {}

    df = _prepare_extra_factors(df)

    # 在历史窗口内, 重新计算每天的评分
    # 简化: 使用当前IC权重对历史数据评分
    recent_window = df.iloc[IC_WINDOW:]
    if len(recent_window) < 50:
        return {}

    # 标准化参数 (使用整个窗口)
    means = {}
    stds = {}
    for col in factor_scores:
        if col in df.columns:
            means[col] = recent_window[col].mean()
            stds[col] = recent_window[col].std()

    # 计算历史每天的简化评分 (与score_trend同尺度)
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
        hist_scores += z * ic_w * 100  # 与score_trend同尺度

    # 归一化到 -100~100 (与score_trend一致)
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

        # 找相似评分的日子 (±0.5 std)
        similar_mask = (hs >= current_score - score_std * 0.5) & (hs <= current_score + score_std * 0.5)
        similar_fwd = fw[similar_mask]

        if len(similar_fwd) < 10:
            similar_mask = (hs >= current_score - score_std) & (hs <= current_score + score_std)
            similar_fwd = fw[similar_mask]

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
