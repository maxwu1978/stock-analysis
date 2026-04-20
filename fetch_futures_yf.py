"""美股期货数据模块 (基于 yfinance)

当前富途账户无 CME/NYMEX/COMEX/CBOT 行情权限,
用 yfinance 免费拉取主力连续合约日线, 足以支持 MF-DFA 分形研究.

yfinance 符号对照:
  指数期货 (CME):      ES=F NQ=F YM=F RTY=F
  微型指数 (CME):       MES=F MNQ=F MYM=F M2K=F
  能源 (NYMEX):         CL=F NG=F HO=F RB=F
  贵金属 (COMEX):       GC=F SI=F HG=F PA=F PL=F
  农产品 (CBOT):        ZC=F ZS=F ZW=F
  货币 (CME):           6E=F 6J=F 6B=F
  VIX 波动率 (CBOE):    VX=F

用法:
  from fetch_futures_yf import fetch_future_history, FUTURES_UNIVERSE
  df = fetch_future_history('ES=F', days=400)
  all_hist = fetch_all_futures(days=400, universe='cme_indexes')
"""

import time
import pandas as pd
import yfinance as yf


FUTURES_UNIVERSE = {
    # 标普/纳指/道指/罗素 - CME 全系
    "cme_indexes": {
        "ES=F": "标普500 E-mini",
        "NQ=F": "纳斯达克100 E-mini",
        "YM=F": "道琼斯 E-mini",
        "RTY=F": "罗素2000 E-mini",
    },
    # 微型合约 - 保证金低, 适合个人测试
    "cme_micro": {
        "MES=F": "微型标普500",
        "MNQ=F": "微型纳斯达克100",
        "MYM=F": "微型道琼斯",
        "M2K=F": "微型罗素2000",
    },
    # 能源
    "energy": {
        "CL=F": "WTI 原油",
        "NG=F": "天然气",
        "HO=F": "取暖油",
        "RB=F": "汽油 RBOB",
    },
    # 贵金属
    "metals": {
        "GC=F": "黄金",
        "SI=F": "白银",
        "HG=F": "铜",
        "PA=F": "钯金",
        "PL=F": "铂金",
    },
    # 农产品
    "agri": {
        "ZC=F": "玉米",
        "ZS=F": "大豆",
        "ZW=F": "小麦",
    },
    # 全部 (做横截面用)
    "all": {},  # 由下面动态填充
}

# 自动合并 all
_all = {}
for key, d in FUTURES_UNIVERSE.items():
    if key != "all":
        _all.update(d)
FUTURES_UNIVERSE["all"] = _all


def fetch_future_history(symbol: str, days: int = 400) -> pd.DataFrame:
    """拉单个期货符号的日线历史.

    返回 DataFrame 与项目其他 fetch 模块兼容:
      列: date, open, high, low, close, volume
      索引: RangeIndex (非日期索引)
    """
    period = f"{max(days, 400)}d" if days < 730 else "5y"
    t = yf.Ticker(symbol)
    hist = t.history(period=period, interval="1d", auto_adjust=False)
    if hist is None or hist.empty:
        return pd.DataFrame()

    # 去掉时区 + 重置索引
    if hist.index.tz is not None:
        hist.index = hist.index.tz_localize(None)

    df = hist.reset_index()
    df.columns = [c.lower() for c in df.columns]
    # yfinance 的 Date 列归一化
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    elif "datetime" in df.columns:
        df["date"] = pd.to_datetime(df["datetime"])
        df.drop(columns=["datetime"], inplace=True)

    # 只保留标准列
    keep = [c for c in ["date", "open", "high", "low", "close", "volume"] if c in df.columns]
    df = df[keep].tail(days).reset_index(drop=True)
    return df


def fetch_all_futures(days: int = 400, universe: str = "cme_indexes",
                      sleep_sec: float = 0.3) -> dict[str, pd.DataFrame]:
    """批量拉取一个 universe 的全部期货. 返回 {symbol: df}"""
    symbols = FUTURES_UNIVERSE.get(universe)
    if symbols is None:
        raise ValueError(f"Unknown universe {universe!r}. Available: {list(FUTURES_UNIVERSE)}")

    out = {}
    for sym, name in symbols.items():
        try:
            df = fetch_future_history(sym, days=days)
            if not df.empty:
                out[sym] = df
                print(f"  {sym:<8} ({name}): {len(df)} 天")
            else:
                print(f"  {sym:<8} ({name}): 无数据")
        except Exception as e:
            print(f"  {sym:<8} FAIL: {str(e)[:60]}")
        time.sleep(sleep_sec)
    return out


if __name__ == "__main__":
    # 自检: 拉 CME 指数期货 400 天历史
    print("测试: CME 指数期货 400 天日线")
    hist = fetch_all_futures(days=400, universe="cme_indexes")
    print(f"\n成功 {len(hist)} 只")
    if hist:
        df = next(iter(hist.values()))
        print(f"示例 ({next(iter(hist))}):")
        print(df.tail(3).to_string())
