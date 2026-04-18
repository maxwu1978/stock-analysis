"""美股数据获取模块 - 基于 yfinance"""

import time
import pandas as pd
import yfinance as yf

US_STOCKS = {
    "NVDA": "英伟达",
    "TSLA": "特斯拉",
    "GOOGL": "谷歌",
    "AAPL": "苹果",
    "TCOM": "携程",
}


def fetch_us_realtime() -> pd.DataFrame:
    """获取美股实时行情"""
    rows = []
    for ticker, name in US_STOCKS.items():
        try:
            t = yf.Ticker(ticker)
            info = t.info
            price = info.get("regularMarketPrice", 0)
            prev = info.get("regularMarketPreviousClose", 0)
            chg = ((price - prev) / prev * 100) if prev else 0
            rows.append({
                "名称": name,
                "代码": ticker,
                "最新价": price,
                "涨跌幅": round(chg, 2),
                "涨跌额": round(price - prev, 2),
                "成交额": info.get("regularMarketVolume", 0),
                "最高": info.get("regularMarketDayHigh", 0),
                "最低": info.get("regularMarketDayLow", 0),
                "昨收": prev,
                "市值": info.get("marketCap", 0),
            })
        except Exception as e:
            print(f"  [!] {name}({ticker}) 行情获取失败: {e}")
    return pd.DataFrame(rows)


def fetch_us_history(ticker: str, period: str = "5y") -> pd.DataFrame:
    """获取单只美股历史日线数据"""
    t = yf.Ticker(ticker)
    df = t.history(period=period)
    df.rename(columns={
        "Open": "open", "Close": "close",
        "High": "high", "Low": "low",
        "Volume": "volume",
    }, inplace=True)
    df.index.name = "date"
    # 去掉时区信息
    df.index = df.index.tz_localize(None)
    df["pct_chg"] = df["close"].pct_change() * 100
    df["change"] = df["close"].diff()
    return df


def fetch_us_all_history(period: str = "5y") -> dict[str, pd.DataFrame]:
    """获取所有美股历史数据"""
    result = {}
    for ticker, name in US_STOCKS.items():
        try:
            result[ticker] = fetch_us_history(ticker, period)
            print(f"  {name}({ticker}): {len(result[ticker])}天")
            time.sleep(0.3)
        except Exception as e:
            print(f"  [!] {name}({ticker}) 历史数据获取失败: {e}")
    return result


def fetch_us_financials() -> dict[str, pd.DataFrame]:
    """获取美股财报数据 (季度)"""
    result = {}
    for ticker, name in US_STOCKS.items():
        try:
            t = yf.Ticker(ticker)
            # 季度财务数据
            q_income = t.quarterly_income_stmt
            q_balance = t.quarterly_balance_sheet

            if q_income is None or q_income.empty:
                continue

            def _safe_get(df_, key, col_):
                """安全提取财报字段, 缺失或NaN返回None"""
                if df_ is None or key not in df_.index or col_ not in df_.columns:
                    return None
                try:
                    v = df_.loc[key, col_]
                    if v is None or pd.isna(v):
                        return None
                    return float(v)
                except (TypeError, ValueError):
                    return None

            def _safe_div(a, b, mult=100):
                if a is None or b is None or b == 0:
                    return None
                return a / b * mult

            rows = []
            for col in q_income.columns:
                report_date = col
                rev = _safe_get(q_income, "Total Revenue", col)
                net = _safe_get(q_income, "Net Income", col)
                gross = _safe_get(q_income, "Gross Profit", col)
                gm = _safe_div(gross, rev)

                total_assets = _safe_get(q_balance, "Total Assets", col)
                total_debt_val = _safe_get(q_balance, "Total Debt", col)
                equity = _safe_get(q_balance, "Stockholders Equity", col)

                roe = _safe_div(net, equity)
                debt_ratio = _safe_div(total_debt_val, total_assets)

                rows.append({
                    "report_date": pd.Timestamp(report_date),
                    "roe": roe,
                    "rev_growth": None,  # 需要同比计算
                    "profit_growth": None,
                    "gross_margin": gm,
                    "debt_ratio": debt_ratio,
                    "revenue": rev,
                    "net_income": net,
                })

            df = pd.DataFrame(rows).sort_values("report_date")

            # 强制数值列为 float, None 转 NaN
            for col_name in ["roe", "gross_margin", "debt_ratio", "revenue", "net_income"]:
                if col_name in df.columns:
                    df[col_name] = pd.to_numeric(df[col_name], errors="coerce")

            # 计算同比增长率 (对比4个季度前)
            if len(df) >= 5:
                df["rev_growth"] = df["revenue"].pct_change(4) * 100
                df["profit_growth"] = df["net_income"].pct_change(4) * 100
            else:
                df["rev_growth"] = pd.NA
                df["profit_growth"] = pd.NA

            # 衍生因子
            df["roe_chg"] = df["roe"].diff()
            df["rev_growth_accel"] = pd.to_numeric(df["rev_growth"], errors="coerce").diff()
            df["cash_flow_ps"] = pd.NA  # yfinance 季度现金流不稳定, 留空

            result[ticker] = df
            print(f"  {name}({ticker}): {len(df)}期财报")
            time.sleep(0.3)
        except Exception as e:
            print(f"  [!] {name}({ticker}) 财报获取失败: {e}")
    return result
