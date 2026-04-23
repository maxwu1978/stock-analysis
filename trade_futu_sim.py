"""富途模拟盘交易测试模块 (SIMULATE ONLY, 硬锁定)

设计原则:
  1. 顶层常量 TRD_ENV = TrdEnv.SIMULATE, 绝不允许切换到 REAL
  2. 下单函数要求命令行参数 --confirm 显式确认
  3. 单笔订单金额上限硬约束 (防止误操作)
  4. 所有下单日志写入 trade_sim_log.csv 留痕

使用方法 (所有命令均由人工执行):
  # 只读操作
  python trade_futu_sim.py balance
  python trade_futu_sim.py positions
  python trade_futu_sim.py orders

  # 下单 (必须加 --confirm)
  python trade_futu_sim.py buy US.AAPL 10 --confirm
  python trade_futu_sim.py sell US.AAPL 5 --confirm
  python trade_futu_sim.py cancel <order_id> --confirm

  # 历史订单
  python trade_futu_sim.py history --days 7
"""

import sys
import csv
import logging
from datetime import datetime, timedelta
from pathlib import Path

logging.getLogger("futu").setLevel(logging.ERROR)

from trade_plan import compose_note, extract_plan_metadata_from_args

# ================== 硬锁定常量 ==================
# TRD_ENV 不得修改: 全模块所有交易调用强制用模拟环境
# 任何通过参数或环境变量尝试切换到 REAL 的代码路径都应该失败
from futu import TrdEnv as _TrdEnv
TRD_ENV = _TrdEnv.SIMULATE   # ← 硬编码, 不从参数读取

# 单笔订单金额上限 (美元, 模拟盘也做约束防止误操作)
MAX_ORDER_VALUE_USD = 50000
MAX_ORDER_VALUE_HKD = 400000

# 期权单笔金额上限 (权利金 × 100 × 张数)
MAX_OPTION_VALUE_USD = 5000

# 日志文件
LOG_PATH = Path(__file__).parent / "trade_sim_log.csv"

def _extract_plan_metadata(args: list[str]) -> dict[str, str]:
    """从命令行提取可选的交易计划元数据.

    兼容旧命令，不要求任何新增参数。
    """
    return extract_plan_metadata_from_args(args)


def _trade_ctx(market: str = "US"):
    """建立交易上下文. market: US | HK"""
    from futu import OpenSecTradeContext, TrdMarket, SecurityFirm
    mkt = {"US": TrdMarket.US, "HK": TrdMarket.HK}[market]
    return OpenSecTradeContext(
        filter_trdmarket=mkt, host="127.0.0.1", port=11111,
        security_firm=SecurityFirm.FUTUSECURITIES,
    )


def _log_action(action: str, code: str, qty: float, price: float, note: str = "") -> None:
    """留痕所有交易动作到 CSV."""
    new_file = not LOG_PATH.exists()
    with LOG_PATH.open("a", newline="") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["timestamp", "action", "code", "qty", "price", "trd_env", "note"])
        w.writerow([
            datetime.now().isoformat(timespec="seconds"),
            action, code, qty, price, "SIMULATE", note,
        ])


# ==================== 只读 ====================

def sim_balance(market: str = "US") -> None:
    from futu import RET_OK
    t = _trade_ctx(market)
    try:
        ret, data = t.accinfo_query(trd_env=TRD_ENV, refresh_cache=True)
        if ret == RET_OK:
            row = data.iloc[0]
            print(f"\n  [SIMULATE {market}] 账户资金")
            print(f"    现金余额      : {row['cash']:>14,.2f}")
            print(f"    总资产        : {row['total_assets']:>14,.2f}")
            print(f"    持仓市值      : {row['market_val']:>14,.2f}")
            print(f"    购买力        : {row['power']:>14,.2f}")
        else:
            print(f"ERR: {data}")
    finally:
        t.close()


def sim_positions(market: str = "US") -> None:
    from futu import RET_OK
    t = _trade_ctx(market)
    try:
        ret, data = t.position_list_query(trd_env=TRD_ENV)
        if ret == RET_OK:
            if len(data) == 0:
                print(f"\n  [SIMULATE {market}] 空仓\n")
                return
            print(f"\n  [SIMULATE {market}] 当前持仓")
            cols = [c for c in ["code", "stock_name", "qty", "cost_price", "nominal_price", "pl_ratio", "pl_val"] if c in data.columns]
            print(data[cols].to_string())
            print()
        else:
            print(f"ERR: {data}")
    finally:
        t.close()


def sim_orders(market: str = "US") -> None:
    """当日订单."""
    from futu import RET_OK
    t = _trade_ctx(market)
    try:
        ret, data = t.order_list_query(trd_env=TRD_ENV)
        if ret == RET_OK:
            if len(data) == 0:
                print(f"\n  [SIMULATE {market}] 今日无订单\n")
                return
            cols = [c for c in ["order_id", "code", "trd_side", "order_type", "order_status", "qty", "price", "create_time", "dealt_qty", "dealt_avg_price"] if c in data.columns]
            print(f"\n  [SIMULATE {market}] 今日订单")
            print(data[cols].to_string())
            print()
        else:
            print(f"ERR: {data}")
    finally:
        t.close()


def sim_history(market: str = "US", days: int = 7) -> None:
    """N天历史订单 (模拟盘不支持 deal_list_query, 改用 history_order_list)."""
    from futu import RET_OK
    t = _trade_ctx(market)
    try:
        start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        end = datetime.now().strftime("%Y-%m-%d")
        ret, data = t.history_order_list_query(trd_env=TRD_ENV, start=start, end=end)
        if ret == RET_OK:
            if len(data) == 0:
                print(f"\n  [SIMULATE {market}] 近{days}日无订单\n")
                return
            cols = [c for c in ["order_id", "code", "trd_side", "qty", "price", "dealt_qty", "dealt_avg_price", "order_status", "create_time"] if c in data.columns]
            print(f"\n  [SIMULATE {market}] 历史订单({days}日)")
            print(data[cols].to_string())
            print()
        else:
            print(f"ERR: {data}")
    finally:
        t.close()


# ==================== 下单 (需 --confirm) ====================

def _check_order_limit(code: str, qty: float, last_price: float) -> None:
    """下单前金额上限检查, 超限直接抛错."""
    market = code.split(".")[0]
    value = qty * last_price
    limit = MAX_ORDER_VALUE_USD if market == "US" else MAX_ORDER_VALUE_HKD
    if value > limit:
        raise ValueError(
            f"单笔订单金额超限: {code} × {qty} × ${last_price} = ${value:,.0f} > ${limit:,.0f}"
        )


def _get_last_price(code: str) -> float:
    """拉最新报价用于下单."""
    from futu import OpenQuoteContext, RET_OK, SubType
    q = OpenQuoteContext(host="127.0.0.1", port=11111)
    try:
        q.subscribe([code], [SubType.QUOTE])
        ret, data = q.get_stock_quote([code])
        if ret != RET_OK:
            raise RuntimeError(f"get_stock_quote failed: {data}")
        return float(data.iloc[0]["last_price"])
    finally:
        q.close()


def sim_market_buy(code: str, qty: float, confirmed: bool = False, plan_meta: dict[str, str] | None = None) -> None:
    """市价买入. 必须 confirmed=True 才真执行."""
    if not confirmed:
        print("未加 --confirm, 拒绝下单")
        return
    from futu import RET_OK, TrdSide, OrderType
    market = code.split(".")[0]
    last_price = _get_last_price(code)
    _check_order_limit(code, qty, last_price)

    t = _trade_ctx(market)
    try:
        ret, data = t.place_order(
            price=last_price,  # 市价单也传参考价
            qty=qty, code=code,
            trd_side=TrdSide.BUY,
            order_type=OrderType.MARKET,
            trd_env=TRD_ENV,
        )
        if ret == RET_OK:
            order_id = data.iloc[0]["order_id"]
            print(f"✓ 市价买入已提交: {code} × {qty}  参考价 ${last_price}  order_id={order_id}")
            _log_action("BUY", code, qty, last_price, compose_note(f"order_id={order_id}", plan_meta))
        else:
            print(f"✗ 下单失败: {data}")
            _log_action("BUY_FAIL", code, qty, last_price, compose_note(str(data), plan_meta))
    finally:
        t.close()


def sim_market_sell(code: str, qty: float, confirmed: bool = False, plan_meta: dict[str, str] | None = None) -> None:
    if not confirmed:
        print("未加 --confirm, 拒绝下单")
        return
    from futu import RET_OK, TrdSide, OrderType
    market = code.split(".")[0]
    last_price = _get_last_price(code)
    _check_order_limit(code, qty, last_price)

    t = _trade_ctx(market)
    try:
        ret, data = t.place_order(
            price=last_price, qty=qty, code=code,
            trd_side=TrdSide.SELL,
            order_type=OrderType.MARKET,
            trd_env=TRD_ENV,
        )
        if ret == RET_OK:
            order_id = data.iloc[0]["order_id"]
            print(f"✓ 市价卖出已提交: {code} × {qty}  参考价 ${last_price}  order_id={order_id}")
            _log_action("SELL", code, qty, last_price, compose_note(f"order_id={order_id}", plan_meta))
        else:
            print(f"✗ 下单失败: {data}")
            _log_action("SELL_FAIL", code, qty, last_price, compose_note(str(data), plan_meta))
    finally:
        t.close()


def _is_option(code: str) -> bool:
    """判断是否期权合约代号 (US.XXX<YYMMDD>C/P<STRIKE>)."""
    parts = code.split(".")
    if len(parts) != 2:
        return False
    # 期权代号: 标的后跟 6 位日期 + C/P + 8 位行权价
    body = parts[1]
    return len(body) > 10 and ("C" in body or "P" in body) and any(c.isdigit() for c in body[-8:])


def _check_option_limit(code: str, qty: float, premium: float) -> None:
    """期权下单金额上限检查. premium 是每股权利金."""
    contract_size = 100  # 标准美股期权 1 张 = 100 股
    value = qty * contract_size * premium
    if value > MAX_OPTION_VALUE_USD:
        raise ValueError(
            f"期权订单金额超限: {code} × {qty}张 × {contract_size} × ${premium} = ${value:,.0f} > ${MAX_OPTION_VALUE_USD:,.0f}"
        )


def sim_option_buy(code: str, qty: int, confirmed: bool = False, plan_meta: dict[str, str] | None = None) -> None:
    """开多期权仓位 (买入 Call 做多 / 买入 Put 做空).
    qty 单位: 张 (1张=100股).
    """
    if not confirmed:
        print("未加 --confirm, 拒绝下单")
        return
    if not _is_option(code):
        print(f"✗ 不是合法期权代号: {code}")
        return
    from futu import RET_OK, TrdSide, OrderType
    premium = _get_last_price(code)
    _check_option_limit(code, qty, premium)

    t = _trade_ctx("US")
    try:
        ret, data = t.place_order(
            price=premium, qty=qty, code=code,
            trd_side=TrdSide.BUY,
            order_type=OrderType.NORMAL,  # 期权用限价单
            trd_env=TRD_ENV,
        )
        if ret == RET_OK:
            order_id = data.iloc[0]["order_id"]
            value = qty * 100 * premium
            print(f"✓ 期权买入已提交: {code} × {qty}张  权利金 ${premium}/股  合约价值 ${value:,.0f}  order_id={order_id}")
            _log_action("OPT_BUY", code, qty, premium, compose_note(f"order_id={order_id}", plan_meta))
        else:
            print(f"✗ 下单失败: {data}")
            _log_action("OPT_BUY_FAIL", code, qty, premium, compose_note(str(data), plan_meta))
    finally:
        t.close()


def sim_option_limit_sell(code: str, qty: int, price: float,
                          time_in_force: str = "DAY",
                          confirmed: bool = False,
                          plan_meta: dict[str, str] | None = None) -> None:
    """挂期权**限价卖单**, 默认 DAY 有效期 (富途模拟盘不支持 GTC).

    ⚠ 模拟盘限制: GTC / STOP 均不支持, 只能用 DAY.
       DAY = 当日有效, 到美股收盘 (16:00 ET) 未成交会被撤销.
       需要每日美股盘前重新挂一次 (可用 launchd 自动化).

    用法: 买入后立即挂高于成本的止盈单, 盘中自动监控到价成交.
    qty: 张
    price: 目标权利金卖出价 (高于当前价)
    time_in_force: DAY (默认, 模拟盘唯一支持) 或 GTC (仅实盘)
    """
    if not confirmed:
        print("未加 --confirm, 拒绝挂单")
        return
    if not _is_option(code):
        print(f"✗ 不是合法期权代号: {code}")
        return
    from futu import RET_OK, TrdSide, OrderType, TimeInForce
    _check_option_limit(code, qty, price)

    tif_map = {"GTC": TimeInForce.GTC, "DAY": TimeInForce.DAY}
    tif = tif_map.get(time_in_force, TimeInForce.DAY)

    t = _trade_ctx("US")
    try:
        ret, data = t.place_order(
            price=price, qty=qty, code=code,
            trd_side=TrdSide.SELL,
            order_type=OrderType.NORMAL,  # 限价
            time_in_force=tif,
            trd_env=TRD_ENV,
        )
        if ret == RET_OK:
            order_id = data.iloc[0]["order_id"]
            print(f"✓ 限价卖挂单 {time_in_force}: {code} × {qty}张 @${price:.2f}  order_id={order_id}")
            _log_action("OPT_LIMIT_SELL_GTC", code, qty, price, compose_note(f"order_id={order_id}, tif={time_in_force}", plan_meta))
        else:
            print(f"✗ 挂单失败: {data}")
            _log_action("OPT_LIMIT_SELL_FAIL", code, qty, price, compose_note(str(data), plan_meta))
    finally:
        t.close()


def sim_option_stop_sell(code: str, qty: int, stop_price: float,
                         confirmed: bool = False,
                         plan_meta: dict[str, str] | None = None) -> None:
    """挂期权**STOP 止损单** - 期权价跌破 stop_price 时触发市价卖.

    用法: 买入后立即挂止损, 跌破 stop_price 时市价强平, 限制最大损失.
    qty: 张
    stop_price: 触发止损的权利金价位 (低于当前价)

    例: 成本 $3.10, 接受 50% 止损 → stop_price=$1.55
        跌到 $1.55 时市价卖出平仓
    """
    if not confirmed:
        print("未加 --confirm, 拒绝挂单")
        return
    if not _is_option(code):
        print(f"✗ 不是合法期权代号: {code}")
        return
    from futu import RET_OK, TrdSide, OrderType
    _check_option_limit(code, qty, stop_price)

    t = _trade_ctx("US")
    try:
        ret, data = t.place_order(
            price=stop_price,  # STOP 单的触发价
            qty=qty, code=code,
            trd_side=TrdSide.SELL,
            order_type=OrderType.STOP,  # 跌破触发市价单
            trd_env=TRD_ENV,
        )
        if ret == RET_OK:
            order_id = data.iloc[0]["order_id"]
            print(f"✓ STOP 止损挂单: {code} × {qty}张 触发价${stop_price:.2f}  order_id={order_id}")
            _log_action("OPT_STOP_SELL", code, qty, stop_price, compose_note(f"order_id={order_id}", plan_meta))
        else:
            print(f"✗ STOP 挂单失败 (可能模拟盘不支持期权STOP): {data}")
            _log_action("OPT_STOP_SELL_FAIL", code, qty, stop_price, compose_note(str(data), plan_meta))
    finally:
        t.close()


def sim_option_sell(code: str, qty: int, confirmed: bool = False, plan_meta: dict[str, str] | None = None) -> None:
    """平仓期权 (卖出已持有合约).
    qty 单位: 张.
    """
    if not confirmed:
        print("未加 --confirm, 拒绝下单")
        return
    if not _is_option(code):
        print(f"✗ 不是合法期权代号: {code}")
        return
    from futu import RET_OK, TrdSide, OrderType
    premium = _get_last_price(code)
    _check_option_limit(code, qty, premium)

    t = _trade_ctx("US")
    try:
        ret, data = t.place_order(
            price=premium, qty=qty, code=code,
            trd_side=TrdSide.SELL,
            order_type=OrderType.NORMAL,
            trd_env=TRD_ENV,
        )
        if ret == RET_OK:
            order_id = data.iloc[0]["order_id"]
            value = qty * 100 * premium
            print(f"✓ 期权卖出已提交: {code} × {qty}张  权利金 ${premium}/股  合约价值 ${value:,.0f}  order_id={order_id}")
            _log_action("OPT_SELL", code, qty, premium, compose_note(f"order_id={order_id}", plan_meta))
        else:
            print(f"✗ 下单失败: {data}")
            _log_action("OPT_SELL_FAIL", code, qty, premium, compose_note(str(data), plan_meta))
    finally:
        t.close()


def sim_cancel(order_id: str, market: str = "US", confirmed: bool = False, plan_meta: dict[str, str] | None = None) -> None:
    if not confirmed:
        print("未加 --confirm, 拒绝撤单")
        return
    from futu import RET_OK, ModifyOrderOp
    t = _trade_ctx(market)
    try:
        ret, data = t.modify_order(
            modify_order_op=ModifyOrderOp.CANCEL,
            order_id=order_id, qty=0, price=0,
            trd_env=TRD_ENV,
        )
        print(f"{'✓' if ret == RET_OK else '✗'} 撤单: {order_id}  {data if ret != RET_OK else ''}")
        _log_action("CANCEL", "-", 0, 0, compose_note(f"order_id={order_id}, ret={ret}", plan_meta))
    finally:
        t.close()


# ==================== CLI ====================

def main():
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        return

    cmd = args[0]
    confirmed = "--confirm" in args
    plan_meta = _extract_plan_metadata(args)

    if cmd == "balance":
        sim_balance("US"); sim_balance("HK")
    elif cmd == "positions":
        sim_positions("US"); sim_positions("HK")
    elif cmd == "orders":
        sim_orders("US"); sim_orders("HK")
    elif cmd == "history":
        days = 7
        for i, a in enumerate(args):
            if a == "--days" and i + 1 < len(args):
                days = int(args[i + 1])
        sim_history("US", days); sim_history("HK", days)
    elif cmd == "buy":
        if len(args) < 3:
            print("用法: buy <code> <qty> --confirm")
            return
        # 自动区分股票/期权
        if _is_option(args[1]):
            sim_option_buy(args[1], int(args[2]), confirmed, plan_meta)
        else:
            sim_market_buy(args[1], float(args[2]), confirmed, plan_meta)
    elif cmd == "sell":
        if len(args) < 3:
            print("用法: sell <code> <qty> --confirm")
            return
        if _is_option(args[1]):
            sim_option_sell(args[1], int(args[2]), confirmed, plan_meta)
        else:
            sim_market_sell(args[1], float(args[2]), confirmed, plan_meta)
    elif cmd == "limit_sell":
        # 限价 DAY 卖单 (止盈挂单, 模拟盘唯一支持的 TIF)
        if len(args) < 4:
            print("用法: limit_sell <option_code> <qty> <target_price> --confirm")
            return
        sim_option_limit_sell(args[1], int(args[2]), float(args[3]), "DAY", confirmed, plan_meta)
    elif cmd == "stop_sell":
        # STOP 止损单
        if len(args) < 4:
            print("用法: stop_sell <option_code> <qty> <stop_price> --confirm")
            return
        sim_option_stop_sell(args[1], int(args[2]), float(args[3]), confirmed, plan_meta)
    elif cmd == "cancel":
        if len(args) < 2:
            print("用法: cancel <order_id> [--market US|HK] --confirm")
            return
        market = "US"
        for i, a in enumerate(args):
            if a == "--market" and i + 1 < len(args):
                market = args[i + 1]
        sim_cancel(args[1], market, confirmed, plan_meta)
    else:
        print(f"未知命令: {cmd}")
        print(__doc__)


if __name__ == "__main__":
    main()
