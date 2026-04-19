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
    """获取美股实时行情 (info失败时回退到历史K线最后一根)"""
    rows = []
    for ticker, name in US_STOCKS.items():
        try:
            t = yf.Ticker(ticker)
            info = {}
            try:
                info = t.info
            except Exception:
                pass

            price = info.get("regularMarketPrice") or 0
            prev = info.get("regularMarketPreviousClose") or 0
            high = info.get("regularMarketDayHigh") or 0
            low = info.get("regularMarketDayLow") or 0
            vol = info.get("regularMarketVolume") or 0
            mcap = info.get("marketCap") or 0

            # info 缺失时, 用近5天历史数据回退
            if not price or not prev:
                hist = t.history(period="5d")
                if len(hist) >= 2:
                    price = float(hist["Close"].iloc[-1])
                    prev = float(hist["Close"].iloc[-2])
                    high = float(hist["High"].iloc[-1])
                    low = float(hist["Low"].iloc[-1])
                    vol = int(hist["Volume"].iloc[-1])

            chg = ((price - prev) / prev * 100) if prev else 0
            rows.append({
                "名称": name,
                "代码": ticker,
                "最新价": price,
                "涨跌幅": round(chg, 2),
                "涨跌额": round(price - prev, 2),
                "成交额": vol,
                "最高": high,
                "最低": low,
                "昨收": prev,
                "市值": mcap,
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
                """安全提取财报字段, 缺失或NaN返回None. key可为str或按优先级的list"""
                if df_ is None or col_ not in df_.columns:
                    return None
                keys = key if isinstance(key, (list, tuple)) else [key]
                for k in keys:
                    if k not in df_.index:
                        continue
                    try:
                        v = df_.loc[k, col_]
                        if v is None or pd.isna(v):
                            continue
                        return float(v)
                    except (TypeError, ValueError):
                        continue
                return None

            def _safe_div(a, b, mult=100):
                if a is None or b is None or b == 0:
                    return None
                return a / b * mult

            rows = []
            for col in q_income.columns:
                report_date = col
                rev = _safe_get(q_income, ["Total Revenue", "Operating Revenue"], col)
                net = _safe_get(q_income, [
                    "Net Income",
                    "Net Income Common Stockholders",
                    "Net Income Including Noncontrolling Interests",
                    "Net Income Continuous Operations",
                    "Net Income From Continuing Operation Net Minority Interest",
                    "Net Income From Continuing And Discontinued Operation",
                ], col)
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
