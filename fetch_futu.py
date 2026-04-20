"""富途 OpenD 实时数据模块

要求: 本地运行 Futu OpenD (/Applications/Futu_OpenD.app), 已登录.
行情端口 127.0.0.1:11111, 交易端口 22222.

该模块**仅读**: quote / kline / account position.
**绝不**调用 place_order / modify_order 等交易写入接口.

用法:
  from fetch_futu import realtime_quotes, get_kline, get_positions
  df = realtime_quotes(['US.AAPL', 'HK.00700'])
  df = get_kline('US.AAPL', days=200)
  pos = get_positions()  # 读当前持仓
"""

import logging
import pandas as pd

logging.getLogger("futu").setLevel(logging.ERROR)

# 默认连接参数. OpenD 10.x 将行情和交易合并到同一端口 11111
HOST = "127.0.0.1"
PORT_QUOTE = 11111
PORT_TRADE = 11111  # OpenD 10.x 统一端口; 旧版用 22222


def _quote_ctx():
    from futu import OpenQuoteContext
    return OpenQuoteContext(host=HOST, port=PORT_QUOTE)


def health_check() -> dict:
    """检查 OpenD 连接状态和登录情况.
    返回 dict: { qot_logined, trd_logined, market_us, market_hk, ... }
    """
    from futu import RET_OK
    q = _quote_ctx()
    try:
        ret, data = q.get_global_state()
        return data if ret == RET_OK else {"error": str(data)}
    finally:
        q.close()


def realtime_quotes(codes: list[str]) -> pd.DataFrame:
    """获取实时行情快照.

    codes 格式: 'US.AAPL', 'HK.00700', 'SH.600519', 'SZ.300750'
    免费账户 A 股无权限, US/HK 可用.

    返回 DataFrame: code, last_price, prev_close_price, change_rate (自算), high_price, low_price, volume, turnover
    """
    from futu import RET_OK, SubType
    q = _quote_ctx()
    try:
        ret, err = q.subscribe(codes, [SubType.QUOTE])
        if ret != RET_OK:
            raise RuntimeError(f"subscribe failed: {err}")
        ret, data = q.get_stock_quote(codes)
        if ret != RET_OK:
            raise RuntimeError(f"get_stock_quote failed: {data}")
        df = data.copy()
        df["change_rate"] = (df["last_price"] / df["prev_close_price"] - 1) * 100
        return df
    finally:
        q.close()


def get_kline(code: str, days: int = 200, ktype: str = "K_DAY") -> pd.DataFrame:
    """拉历史 K 线.

    code: 'US.AAPL' / 'HK.00700' 等富途格式
    days: 天数 (近似, 按自然日往前回溯)
    ktype: K_DAY / K_WEEK / K_MON / K_60M / K_30M / K_15M / K_5M / K_1M

    返回 DataFrame: time_key, open, close, high, low, volume, turnover, pe_ratio, turnover_rate
    """
    from futu import RET_OK, KLType
    from datetime import datetime, timedelta
    ktype_map = {
        "K_DAY": KLType.K_DAY, "K_WEEK": KLType.K_WEEK, "K_MON": KLType.K_MON,
        "K_60M": KLType.K_60M, "K_30M": KLType.K_30M, "K_15M": KLType.K_15M,
        "K_5M": KLType.K_5M, "K_1M": KLType.K_1M,
    }
    end = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=days * 1.5)).strftime("%Y-%m-%d")  # 留buffer

    q = _quote_ctx()
    try:
        ret, data, _ = q.request_history_kline(
            code, start=start, end=end, ktype=ktype_map.get(ktype, ktype),
            max_count=min(1000, days * 2)
        )
        if ret != RET_OK:
            raise RuntimeError(f"request_history_kline failed: {data}")
        # 保持最多 days 条
        if len(data) > days:
            data = data.tail(days).reset_index(drop=True)
        return data
    finally:
        q.close()


def get_positions(trd_env: str = "REAL") -> pd.DataFrame:
    """读取当前持仓. 需要 OpenD 已登录 + 交易已解锁.

    OpenD 10.x 版本行情+交易统一端口 11111, 旧版用 22222.
    trd_env: 'REAL' 真实账户 | 'SIMULATE' 模拟账户

    返回 DataFrame: code, stock_name, qty, cost_price, nominal_price, pl_ratio, market_val, ...

    **只读**: 此函数绝不执行下单等写入操作.
    """
    from futu import OpenSecTradeContext, RET_OK, TrdEnv, TrdMarket, SecurityFirm
    t = OpenSecTradeContext(
        filter_trdmarket=TrdMarket.US,  # filter 参数为必填, 实际返回全账户持仓
        host=HOST, port=PORT_TRADE,
        security_firm=SecurityFirm.FUTUSECURITIES,
    )
    try:
        env = TrdEnv.REAL if trd_env == "REAL" else TrdEnv.SIMULATE
        ret, data = t.position_list_query(trd_env=env)
        if ret != RET_OK:
            return pd.DataFrame({"error": [str(data)]})
        return data if not data.empty else pd.DataFrame()
    finally:
        t.close()


def get_position_codes() -> list[str]:
    """持仓代码列表, 用于实盘观察关注池."""
    df = get_positions()
    if df.empty or "code" not in df.columns:
        return []
    return df["code"].tolist()


if __name__ == "__main__":
    # 简单自检
    print("=== Health Check ===")
    hc = health_check()
    for k in ("qot_logined", "trd_logined", "market_us", "market_hk", "market_sh", "program_status_type"):
        print(f"  {k}: {hc.get(k)}")

    print("\n=== US Realtime ===")
    try:
        df = realtime_quotes(["US.AAPL", "US.NVDA", "US.TSLA"])
        print(df[["code", "last_price", "change_rate", "volume"]].to_string())
    except Exception as e:
        print(f"  FAIL: {e}")

    print("\n=== HK Realtime ===")
    try:
        df = realtime_quotes(["HK.00700", "HK.09988"])
        print(df[["code", "last_price", "change_rate", "volume"]].to_string())
    except Exception as e:
        print(f"  FAIL: {e}")

    print("\n=== Positions (US) ===")
    try:
        pos = get_positions()
        if "error" in pos.columns:
            print(f"  {pos.iloc[0]['error']}")
        else:
            print(pos[["code", "stock_name", "qty", "cost_price", "pl_ratio"]].to_string() if not pos.empty else "  (empty)")
    except Exception as e:
        print(f"  FAIL: {e}")
