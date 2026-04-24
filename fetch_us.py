"""美股数据获取模块

默认采用多源回退:
1. Futu OpenD: 盘中实时与历史K线优先, 与本地交易终端口径更一致
2. yfinance: 通用回退, 也负责财报数据
"""

import time
from datetime import datetime

import pandas as pd
import yfinance as yf

US_STOCKS = {
    "NVDA": "英伟达",
    "TSM": "台积电",
    "MU": "美光科技",
    "WDC": "西部数据",
    "TSLA": "特斯拉",
    "GOOGL": "谷歌",
    "AAPL": "苹果",
    "TCOM": "携程",
    "FUTU": "富途控股",
}


def _fetch_futu_realtime() -> pd.DataFrame:
    """优先用 Futu 获取实时美股行情."""
    from fetch_futu import realtime_quotes

    codes = [f"US.{ticker}" for ticker in US_STOCKS]
    df = realtime_quotes(codes)
    if df.empty:
        return pd.DataFrame()

    rows = []
    for _, r in df.iterrows():
        raw_code = str(r.get("code", ""))
        ticker = raw_code.split(".")[-1]
        if ticker not in US_STOCKS:
            continue
        last_price = float(r.get("last_price", 0) or 0)
        prev_close = float(r.get("prev_close_price", 0) or 0)
        high = float(r.get("high_price", 0) or 0)
        low = float(r.get("low_price", 0) or 0)
        volume = float(r.get("volume", 0) or 0)
        chg = ((last_price - prev_close) / prev_close * 100) if prev_close else 0
        rows.append({
            "名称": US_STOCKS[ticker],
            "代码": ticker,
            "最新价": last_price,
            "涨跌幅": round(chg, 2),
            "涨跌额": round(last_price - prev_close, 2),
            "成交额": volume,
            "最高": high,
            "最低": low,
            "昨收": prev_close,
            "市值": 0,  # Futu 快照这里不稳定返回市值, 交给 yfinance 补
            "数据源": "futu",
            "更新时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
    return pd.DataFrame(rows)


def _fast_info_dict(ticker_obj) -> dict:
    try:
        return dict(ticker_obj.fast_info)
    except Exception:
        return {}


def _first_present(mapping: dict, *keys):
    for key in keys:
        value = mapping.get(key)
        if value is not None and not pd.isna(value):
            return value
    return None


def _quote_time_text(index_value) -> str:
    try:
        ts = pd.Timestamp(index_value)
        if ts.tzinfo is not None:
            return ts.strftime("%Y-%m-%d %H:%M %Z")
        return ts.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return ""


def _fetch_yfinance_realtime() -> pd.DataFrame:
    """yfinance 实时行情回退.

    `Ticker.info` 在 GitHub Actions 上有时落后或字段缺失, 所以优先使用
    fast_info 和 1分钟K线快照, 再回退到 info / 日线。
    """
    rows = []
    for ticker, name in US_STOCKS.items():
        try:
            t = yf.Ticker(ticker)
            fast = _fast_info_dict(t)
            info = {}

            price = _first_present(fast, "lastPrice")
            prev = _first_present(fast, "previousClose", "regularMarketPreviousClose")
            high = _first_present(fast, "dayHigh")
            low = _first_present(fast, "dayLow")
            vol = _first_present(fast, "lastVolume")
            mcap = _first_present(fast, "marketCap")
            quote_time = ""

            try:
                intraday = t.history(period="1d", interval="1m", prepost=True)
            except Exception:
                intraday = pd.DataFrame()
            if intraday is not None and not intraday.empty:
                last_row = intraday.dropna(subset=["Close"]).iloc[-1]
                price = float(last_row["Close"])
                high = float(intraday["High"].max(skipna=True))
                low = float(intraday["Low"].min(skipna=True))
                vol = int(intraday["Volume"].sum(skipna=True))
                quote_time = _quote_time_text(intraday.index[-1])

            if not price or not prev or not mcap:
                try:
                    info = t.info
                except Exception:
                    info = {}
                price = price or info.get("regularMarketPrice") or 0
                prev = prev or info.get("regularMarketPreviousClose") or 0
                high = high or info.get("regularMarketDayHigh") or 0
                low = low or info.get("regularMarketDayLow") or 0
                vol = vol or info.get("regularMarketVolume") or 0
                mcap = mcap or info.get("marketCap") or 0

            if not price or not prev:
                hist = t.history(period="5d")
                if len(hist) >= 2:
                    price = float(hist["Close"].iloc[-1])
                    prev = float(hist["Close"].iloc[-2])
                    high = float(hist["High"].iloc[-1])
                    low = float(hist["Low"].iloc[-1])
                    vol = int(hist["Volume"].iloc[-1])
                    quote_time = quote_time or _quote_time_text(hist.index[-1])

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
                "数据源": "yfinance",
                "更新时间": quote_time,
            })
        except Exception as e:
            print(f"  [!] {name}({ticker}) 行情获取失败: {e}")
    return pd.DataFrame(rows)


def fetch_us_realtime() -> pd.DataFrame:
    """获取美股实时行情.

    优先用 Futu OpenD, 失败或缺失时退回 yfinance.
    """
    try:
        futu_df = _fetch_futu_realtime()
    except Exception as e:
        print(f"  [!] Futu 美股行情失败, 回退 yfinance: {e}")
        futu_df = pd.DataFrame()

    if futu_df.empty:
        return _fetch_yfinance_realtime()

    # 用 yfinance 尝试补全市值等 Futu 缺失字段, 失败不影响主结果
    try:
        yf_df = _fetch_yfinance_realtime()
        if not yf_df.empty:
            futu_df = futu_df.set_index("代码")
            yf_df = yf_df.set_index("代码")
            for col in ["市值", "更新时间"]:
                if col in yf_df.columns:
                    if col == "市值":
                        futu_df[col] = futu_df[col].where(futu_df[col].astype(float) != 0, yf_df[col])
                    else:
                        futu_df[col] = futu_df[col].where(futu_df[col].astype(str).str.len() > 0, yf_df[col])
            futu_df = futu_df.reset_index()
            futu_df = futu_df[["名称", "代码", "最新价", "涨跌幅", "涨跌额", "成交额", "最高", "最低", "昨收", "市值", "数据源", "更新时间"]]
    except Exception:
        pass
    return futu_df


def fetch_us_history(ticker: str, period: str = "5y") -> pd.DataFrame:
    """获取单只美股历史日线数据.

    优先 Futu K线, 回退到 yfinance.
    """
    try:
        from fetch_futu import get_kline

        # 将 period 粗映射为交易日长度
        days_map = {"1y": 260, "2y": 520, "3y": 780, "5y": 1300, "10y": 2600}
        days = days_map.get(period, 1300)
        df = get_kline(f"US.{ticker}", days=days)
        if df is not None and not df.empty:
            out = df.copy()
            if "time_key" in out.columns:
                out["date"] = pd.to_datetime(out["time_key"]).dt.tz_localize(None)
                out = out.set_index("date")
            out = out.rename(columns={"turnover": "turnover"})
            out = out[["open", "close", "high", "low", "volume"]].copy()
            out["pct_chg"] = out["close"].pct_change() * 100
            out["change"] = out["close"].diff()
            return out
    except Exception as e:
        print(f"  [!] {ticker} Futu历史数据失败, 回退 yfinance: {e}")

    t = yf.Ticker(ticker)
    df = t.history(period=period)
    df.rename(columns={
        "Open": "open", "Close": "close",
        "High": "high", "Low": "low",
        "Volume": "volume",
    }, inplace=True)
    df.index.name = "date"
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
                df["rev_growth"] = df["revenue"].pct_change(4, fill_method=None) * 100
                df["profit_growth"] = df["net_income"].pct_change(4, fill_method=None) * 100
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
