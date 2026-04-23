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
import time
import pandas as pd

logging.getLogger("futu").setLevel(logging.ERROR)

# 默认连接参数. OpenD 10.x 将行情和交易合并到同一端口 11111
HOST = "127.0.0.1"
PORT_QUOTE = 11111
PORT_TRADE = 11111  # OpenD 10.x 统一端口; 旧版用 22222


def _quote_ctx():
    from futu import OpenQuoteContext
    return OpenQuoteContext(host=HOST, port=PORT_QUOTE)


def _is_rate_limited(err) -> bool:
    msg = str(err).lower()
    return "too frequent" in msg or "no more request" in msg


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


# ==================== 期权 ====================

def get_option_expirations(underlying: str) -> pd.DataFrame:
    """获取标的股的期权到期日列表.

    underlying: 'US.NVDA' / 'US.AAPL' 等
    返回: DataFrame(strike_time, option_expiry_date_distance, expiration_cycle)
    """
    from futu import RET_OK
    q = _quote_ctx()
    try:
        last_err = None
        for delay in (0.0, 0.4, 0.8, 1.6):
            if delay:
                time.sleep(delay)
            ret, data = q.get_option_expiration_date(underlying)
            if ret == RET_OK:
                return data
            last_err = data
            if not _is_rate_limited(data):
                raise RuntimeError(f"get_option_expiration_date failed: {data}")
        raise RuntimeError(f"get_option_expiration_date failed: {last_err}")
    finally:
        q.close()


def get_option_chain_full(
    underlying: str,
    expiry: str,
    *,
    spot: float | None = None,
    strike_band: float | None = None,
    max_contracts_per_side: int | None = 6,
) -> pd.DataFrame:
    """获取指定到期日的期权链 + 实时报价 + 希腊字母.

    underlying: 'US.NVDA'
    expiry: '2026-04-24' (到期日, 来自 get_option_expirations)
    spot/strike_band: 若提供, 会先按 ATM 附近行权价过滤, 再订阅 snapshot
    max_contracts_per_side: 每侧最多保留多少个合约, 降低订阅额度消耗

    返回 DataFrame 列:
      code, name, option_type (CALL/PUT), strike_price, last_price, prev_close_price,
      bid_price, ask_price, volume, open_interest,
      iv, delta, gamma, theta, vega, days_to_expiry
    """
    from futu import RET_OK, SubType
    q = _quote_ctx()
    try:
        # 1. 拉期权链 (基础结构)
        last_err = None
        chain = None
        for delay in (0.0, 1.0, 2.0, 4.0):
            if delay:
                time.sleep(delay)
            ret, chain = q.get_option_chain(underlying, start=expiry, end=expiry)
            if ret == RET_OK:
                break
            last_err = chain
            if not _is_rate_limited(chain):
                raise RuntimeError(f"get_option_chain failed: {chain}")
        if ret != RET_OK:
            raise RuntimeError(f"get_option_chain failed: {last_err}")
        if chain.empty:
            return pd.DataFrame()

        chain = chain.copy()
        if spot and strike_band is not None:
            k_lo = spot * (1 - strike_band)
            k_hi = spot * (1 + strike_band)
            chain = chain[(chain["strike_price"] >= k_lo) & (chain["strike_price"] <= k_hi)].copy()

        if chain.empty:
            return pd.DataFrame()

        if spot:
            chain["moneyness_abs"] = (chain["strike_price"] / spot - 1).abs()
        else:
            chain["moneyness_abs"] = 0.0

        if max_contracts_per_side:
            slices = []
            for option_type in ("CALL", "PUT"):
                side = chain[chain["option_type"] == option_type].copy()
                if side.empty:
                    continue
                side = side.sort_values(["moneyness_abs", "strike_price"])
                slices.append(side.head(max_contracts_per_side))
            if slices:
                chain = pd.concat(slices, ignore_index=True)

        chain = chain.drop_duplicates(subset=["code"]).reset_index(drop=True)
        codes = chain["code"].tolist()

        # 2. 订阅并拉 snapshot (含希腊字母)
        # 先筛后订, 把单标的订阅从整条链压到 ATM 附近少量合约
        BATCH = 20
        snapshots = []
        for i in range(0, len(codes), BATCH):
            batch = codes[i:i + BATCH]
            ret, err = q.subscribe(batch, [SubType.QUOTE])
            if ret != RET_OK:
                raise RuntimeError(f"subscribe failed: {err}")
            try:
                ret, snap = q.get_market_snapshot(batch)
                if ret == RET_OK and not snap.empty:
                    snapshots.append(snap)
            finally:
                try:
                    q.unsubscribe(batch, [SubType.QUOTE])
                except Exception:
                    pass
        if not snapshots:
            return pd.DataFrame()

        snap_df = pd.concat(snapshots, ignore_index=True)

        # 3. 先保留链路基础字段, 再合并 snapshot 核心列
        df = chain[["code", "name", "option_type", "strike_price", "strike_time"]].copy()
        core_cols = {
            "code": "code",
            "option_strike_price": "strike_price",
            "last_price": "last_price",
            "prev_close_price": "prev_close",
            "bid_price": "bid",
            "ask_price": "ask",
            "volume": "volume",
            "option_open_interest": "open_interest",
            "option_implied_volatility": "iv",
            "option_delta": "delta",
            "option_gamma": "gamma",
            "option_theta": "theta",
            "option_vega": "vega",
            "option_expiry_date_distance": "days_to_expiry",
        }
        snap_core = snap_df[[k for k in core_cols if k in snap_df.columns]].copy()
        snap_core.rename(columns=core_cols, inplace=True)
        df = df.merge(snap_core, on="code", how="left", suffixes=("", "_snap"))
        if "strike_price_snap" in df.columns:
            df["strike_price"] = df["strike_price_snap"].fillna(df["strike_price"])
            df.drop(columns=["strike_price_snap"], inplace=True)
        df.rename(columns={"strike_time": "expiry"}, inplace=True)
        # 变化率
        if "last_price" in df.columns and "prev_close" in df.columns:
            df["chg_pct"] = (df["last_price"] / df["prev_close"] - 1) * 100
        return df.sort_values(["option_type", "strike_price"]).reset_index(drop=True)
    finally:
        q.close()


def find_atm_options(underlying: str, days_to_expiry: int = 7,
                     strike_band: float = 0.05,
                     max_contracts_per_side: int = 6) -> pd.DataFrame:
    """按到期距离和 moneyness 查找 ATM 附近的期权.

    underlying: 'US.NVDA'
    days_to_expiry: 期望天数 (匹配最接近的到期日)
    strike_band: 行权价偏离现价的比例 (默认 ±5% → 显示现价±5% 范围)

    返回: ATM 附近的 Call + Put 列表 (可用于策略决策)
    """
    from futu import RET_OK, SubType
    # 获取底层现价
    q = _quote_ctx()
    try:
        q.subscribe([underlying], [SubType.QUOTE])
        ret, data = q.get_stock_quote([underlying])
        if ret != RET_OK or data.empty:
            raise RuntimeError(f"无法获取 {underlying} 现价")
        spot = float(data.iloc[0]["last_price"])
    finally:
        q.close()

    # 找最接近目标天数的到期日
    exps = get_option_expirations(underlying)
    if exps.empty:
        return pd.DataFrame()
    exps = exps.copy()
    exps["dist"] = (exps["option_expiry_date_distance"] - days_to_expiry).abs()
    target_expiry = exps.loc[exps["dist"].idxmin(), "strike_time"]

    # 拉 ATM 附近小范围期权链, 避免整条链订阅打满行情额度
    chain = get_option_chain_full(
        underlying,
        target_expiry,
        spot=spot,
        strike_band=strike_band,
        max_contracts_per_side=max_contracts_per_side,
    )
    if chain.empty:
        return pd.DataFrame()

    chain["spot"] = spot
    chain["moneyness_pct"] = (chain["strike_price"] / spot - 1) * 100
    return chain.reset_index(drop=True)


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
