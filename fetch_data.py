"""A股数据获取模块 - Sina API (实时) + 腾讯财经 (历史K线)"""

import re
import time
import requests
import pandas as pd
from datetime import datetime, timedelta

STOCKS = {
    "300750": "宁德时代",
    "600519": "贵州茅台",
    "601600": "中国铝业",
    "300274": "阳光电源",
    "600745": "闻泰科技",
}

# 股票代码 -> Sina 格式 (sh/sz + code)
def _sina_symbol(code: str) -> str:
    return f"sz{code}" if code.startswith("3") or code.startswith("0") else f"sh{code}"


def fetch_realtime_quotes() -> pd.DataFrame:
    """通过新浪财经 API 获取实时行情"""
    symbols = ",".join(_sina_symbol(c) for c in STOCKS)
    url = f"https://hq.sinajs.cn/list={symbols}"
    headers = {"Referer": "https://finance.sina.com.cn"}
    resp = requests.get(url, headers=headers, timeout=15)
    resp.encoding = "gbk"

    rows = []
    for line in resp.text.strip().split("\n"):
        m = re.match(r'var hq_str_(s[hz]\d+)="(.+)"', line)
        if not m:
            continue
        sina_code = m.group(1)
        code = sina_code[2:]
        fields = m.group(2).split(",")
        if len(fields) < 32:
            continue

        name = fields[0]
        rows.append({
            "代码": code,
            "名称": STOCKS.get(code, name),
            "最新价": float(fields[3]) if fields[3] else 0,
            "今开": float(fields[1]) if fields[1] else 0,
            "昨收": float(fields[2]) if fields[2] else 0,
            "最高": float(fields[4]) if fields[4] else 0,
            "最低": float(fields[5]) if fields[5] else 0,
            "成交量(手)": int(float(fields[8]) / 100) if fields[8] else 0,
            "成交额": float(fields[9]) if fields[9] else 0,
            "日期": fields[30],
            "时间": fields[31],
        })

    df = pd.DataFrame(rows)
    if not df.empty and "昨收" in df.columns:
        df["涨跌幅"] = ((df["最新价"] - df["昨收"]) / df["昨收"] * 100).round(2)
        df["涨跌额"] = (df["最新价"] - df["昨收"]).round(2)
    return df


def fetch_history(symbol: str, days: int = 120) -> pd.DataFrame:
    """获取单只股票的历史日线数据（前复权）- 使用腾讯财经 API"""
    sina_sym = _sina_symbol(symbol)
    url = f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={sina_sym},day,,,{days},qfq"
    resp = requests.get(url, timeout=15)
    data = resp.json()

    klines = []
    if "data" in data and sina_sym in data["data"]:
        klines = data["data"][sina_sym].get("qfqday") or data["data"][sina_sym].get("day", [])

    if not klines:
        raise ValueError(f"No historical data for {symbol}")

    rows = []
    for k in klines:
        rows.append({
            "date": k[0],
            "open": float(k[1]),
            "close": float(k[2]),
            "high": float(k[3]),
            "low": float(k[4]),
            "volume": int(float(k[5])) if len(k) > 5 else 0,
        })

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)

    # 计算涨跌幅
    df["pct_chg"] = df["close"].pct_change() * 100
    df["change"] = df["close"].diff()
    return df


def fetch_all_history(days: int = 120) -> dict[str, pd.DataFrame]:
    """获取所有目标股票的历史数据"""
    result = {}
    for code, name in STOCKS.items():
        try:
            result[code] = fetch_history(code, days)
            time.sleep(0.2)
        except Exception as e:
            print(f"  [!] {name}({code}) 历史数据获取失败: {e}")
    return result
