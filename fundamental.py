"""财报数据获取与处理模块"""

import re
import time
import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from fetch_data import STOCKS


def _parse_value(val):
    """解析同花顺财务数据中的各种格式"""
    if val is None or val is False or (isinstance(val, float) and np.isnan(val)):
        return np.nan
    s = str(val).strip()
    if s in ("", "False", "None", "--", "-"):
        return np.nan
    # 百分比
    if s.endswith("%"):
        try:
            return float(s[:-1])
        except ValueError:
            return np.nan
    # 亿/万
    m = re.match(r"^([+-]?\d+\.?\d*)(亿|万)?$", s)
    if m:
        num = float(m.group(1))
        unit = m.group(2)
        if unit == "亿":
            return num * 1e8
        elif unit == "万":
            return num * 1e4
        return num
    try:
        return float(s)
    except ValueError:
        return np.nan


def fetch_financial(code: str) -> pd.DataFrame:
    """获取单只股票的财务摘要数据 (同花顺)

    返回按报告期排序的DataFrame, 数值已解析为float
    """
    try:
        raw = ak.stock_financial_abstract_ths(symbol=code)
    except Exception as e:
        print(f"  [!] {code} 财报获取失败: {e}")
        return pd.DataFrame()

    if raw.empty:
        return pd.DataFrame()

    # 解析报告期
    raw["报告期"] = pd.to_datetime(raw["报告期"], errors="coerce")
    raw = raw.dropna(subset=["报告期"]).sort_values("报告期")

    # 选择需要的列并解析
    cols_map = {
        "净资产收益率": "roe",
        "营业总收入同比增长率": "rev_growth",
        "净利润同比增长率": "profit_growth",
        "销售毛利率": "gross_margin",
        "资产负债率": "debt_ratio",
        "每股经营现金流": "cash_flow_ps",
        "每股净资产": "bps",
        "基本每股收益": "eps",
    }

    result = pd.DataFrame({"report_date": raw["报告期"]})
    for cn_col, en_col in cols_map.items():
        if cn_col in raw.columns:
            result[en_col] = raw[cn_col].apply(_parse_value)

    # 衍生因子: ROE环比变化, 营收加速度
    result["roe_chg"] = result["roe"].diff()
    result["rev_growth_accel"] = result["rev_growth"].diff()

    result = result.reset_index(drop=True)
    return result


def align_fundamental_to_daily(fund_df: pd.DataFrame, daily_df: pd.DataFrame,
                                disclosure_lag_days: int = 60) -> pd.DataFrame:
    """将季度财报数据对齐到日线DataFrame

    为避免前视偏差, 假设财报在报告期后 disclosure_lag_days 天才可用。
    例: 2024Q3报告期=2024-09-30, 假设2024-11-29后才能使用。

    在两次财报之间, 数据保持不变 (forward-fill)。
    """
    if fund_df.empty or daily_df.empty:
        return daily_df

    fund_cols = [c for c in fund_df.columns if c != "report_date"]

    # 添加可用日期
    fund_df = fund_df.copy()
    fund_df["available_date"] = fund_df["report_date"] + timedelta(days=disclosure_lag_days)

    # 对每个交易日, 找到最近一期已可用的财报
    for col in fund_cols:
        daily_df[col] = np.nan

    for i, frow in fund_df.iterrows():
        avail = frow["available_date"]
        mask = daily_df.index >= avail
        if not mask.any():
            continue
        # 找到下一期的可用日期
        next_rows = fund_df[fund_df["available_date"] > avail]
        if not next_rows.empty:
            next_avail = next_rows.iloc[0]["available_date"]
            mask = mask & (daily_df.index < next_avail)
        for col in fund_cols:
            val = frow.get(col)
            if pd.notna(val):
                daily_df.loc[mask, col] = val

    return daily_df


def fetch_all_financials() -> dict[str, pd.DataFrame]:
    """获取所有目标股票的财报数据"""
    result = {}
    for code, name in STOCKS.items():
        try:
            df = fetch_financial(code)
            if not df.empty:
                result[code] = df
                print(f"  {name}({code}): {len(df)}期财报")
            time.sleep(0.5)
        except Exception as e:
            print(f"  [!] {name}({code}) 财报失败: {e}")
    return result
