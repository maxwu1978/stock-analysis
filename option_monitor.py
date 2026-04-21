"""期权持仓监控 — 定时跑, 输出每个期权的 PnL/Greeks/剩余天数/盈亏平衡距离

功能:
  1. 读模拟盘的期权持仓 (get_positions)
  2. 实时拉期权行情 + 希腊字母
  3. 聚合跨式 (同日期同strike的Call+Put)
  4. 计算: 当前 PnL / Theta 日损 / 距盈亏平衡 / 到期剩余天数
  5. 输出格式化文本 + 追加到 option_status.log + 发 macOS 通知

用法:
  python option_monitor.py           # 输出到控制台 + log + 通知
  python option_monitor.py --quiet   # 只写log, 不通知 (用于频繁跑)
"""

import sys
import subprocess
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

from fetch_futu import get_positions, realtime_quotes


LOG_PATH = Path(__file__).parent / "option_status.log"
LOG_TEXT_PATH = Path(__file__).parent / "option_status_latest.txt"


def parse_option_code(code: str) -> dict | None:
    """解析期权代号: US.NVDA260424P200000 → {underlying, expiry, type, strike}"""
    m = re.match(r"^(US|HK)\.([A-Z]+)(\d{6})([CP])(\d+)$", code)
    if not m:
        return None
    mkt, sym, ymd, ctype, strike_raw = m.groups()
    y = 2000 + int(ymd[:2])
    mo = int(ymd[2:4])
    d = int(ymd[4:6])
    expiry = f"{y:04d}-{mo:02d}-{d:02d}"
    return {
        "underlying": f"{mkt}.{sym}",
        "expiry": expiry,
        "option_type": "CALL" if ctype == "C" else "PUT",
        "strike": int(strike_raw) / 1000,  # 200000 → 200
    }


def analyze_positions(trd_env: str = "SIMULATE") -> list[dict]:
    """分析当前所有期权持仓. 默认看模拟盘."""
    pos = get_positions(trd_env=trd_env)
    if pos.empty:
        return []

    # 只取期权 (qty > 0 且代号带 C/P)
    pos = pos[pos["qty"] > 0].copy() if "qty" in pos.columns else pos
    options = []
    for _, r in pos.iterrows():
        info = parse_option_code(r["code"])
        if info is None:
            continue
        options.append({
            "code": r["code"],
            "name": r["stock_name"],
            "qty": r["qty"],
            "cost_price": r["cost_price"],
            "current_price": r["nominal_price"],
            "pl_ratio": r["pl_ratio"],
            "pl_val": r["pl_val"],
            **info,
        })

    if not options:
        return []

    # 拉最新行情 (含希腊字母)
    from futu import OpenQuoteContext, RET_OK, SubType
    q = OpenQuoteContext(host="127.0.0.1", port=11111)
    try:
        codes = [o["code"] for o in options]
        q.subscribe(codes, [SubType.QUOTE])
        ret, snap = q.get_market_snapshot(codes)
        if ret == RET_OK:
            snap = snap.set_index("code")
            for o in options:
                if o["code"] in snap.index:
                    s = snap.loc[o["code"]]
                    o["iv"] = s.get("option_implied_volatility", 0)
                    o["delta"] = s.get("option_delta", 0)
                    o["gamma"] = s.get("option_gamma", 0)
                    o["theta"] = s.get("option_theta", 0)
                    o["vega"] = s.get("option_vega", 0)
                    o["days_to_expiry"] = s.get("option_expiry_date_distance", 0)

        # 也拉底层现价
        underlyings = list({o["underlying"] for o in options})
        q.subscribe(underlyings, [SubType.QUOTE])
        ret, under_snap = q.get_stock_quote(underlyings)
        if ret == RET_OK:
            under_snap = under_snap.set_index("code")
            for o in options:
                if o["underlying"] in under_snap.index:
                    o["spot"] = float(under_snap.loc[o["underlying"]]["last_price"])
    finally:
        q.close()

    return options


def classify_straddle_action(s: dict) -> dict:
    """根据当前状态给出跨式平仓建议.

    基于专业期权交易规则 (优先级从高到低):
      1. 利润锁: PnL ≥ +50% → 平仓锁利
      2. 突破: 股价越过 breakeven → 已入盈利区, 获利了结
      3. 紧急: 剩余 ≤3天 且亏损 → 立即平仓止血
      4. 止损: PnL ≤ -50% → 止损
      5. 提前获利: 盈利 ≥30% 且 ≤10 天 → 考虑平仓
      6. 21天警戒: 剩余 ≤21 天进入 theta 加速区
      7. 其他: 继续持有
    """
    pl_pct = s.get("pl_pct", 0) or 0
    days = int(s.get("days_to_expiry", 0))
    spot = s.get("spot", 0)
    be_up = s.get("breakeven_upper", 0)
    be_dn = s.get("breakeven_lower", 0)

    beyond_be = spot > be_up or spot < be_dn

    if pl_pct >= 50:
        return {"action": "TAKE_PROFIT", "level": "ACT",
                "reason": f"盈利 {pl_pct:+.1f}% ≥ 50%, 50%法则建议平仓锁利"}
    if beyond_be and pl_pct > 15:
        return {"action": "TAKE_PROFIT", "level": "ACT",
                "reason": f"股价 ${spot:.2f} 已越过盈亏平衡, 锁利出场"}
    if days <= 3 and pl_pct < -5:
        return {"action": "CLOSE_URGENT", "level": "ACT",
                "reason": f"到期仅 {days} 天且亏损 {pl_pct:+.1f}%, 紧急平仓"}
    if pl_pct <= -50:
        return {"action": "STOP_LOSS", "level": "ACT",
                "reason": f"亏损 {pl_pct:+.1f}% ≤ -50%, 止损出场"}
    if pl_pct >= 30 and days <= 10:
        return {"action": "CONSIDER_CLOSE", "level": "WARN",
                "reason": f"盈利 {pl_pct:+.1f}% 且剩 {days} 天, 可考虑提前了结"}
    if days <= 7 and pl_pct < -20:
        return {"action": "STOP_LOSS", "level": "WARN",
                "reason": f"剩 {days} 天+亏损 {pl_pct:+.1f}%, 建议止损"}
    if days <= 7:
        return {"action": "WATCH_CLOSE", "level": "WARN",
                "reason": f"剩 {days} 天, theta 加速区, 每日监控"}
    if days <= 21:
        return {"action": "HOLD_CAUTION", "level": "NOTE",
                "reason": f"剩 {days} 天, 进入 21天警戒区"}
    return {"action": "HOLD", "level": "OK",
            "reason": f"剩 {days} 天, PnL {pl_pct:+.1f}%, 继续持有"}


def classify_solo_option_action(o: dict) -> dict:
    """独腿期权平仓建议 (买入方向, Call/Put 做多). 单腿更简单."""
    pl_ratio = (o.get("pl_ratio") or 0) * 100
    days = int(o.get("days_to_expiry", 0))

    if pl_ratio >= 100:
        return {"action": "TAKE_PROFIT", "level": "ACT",
                "reason": f"翻倍+ ({pl_ratio:+.1f}%), 强烈建议部分平仓锁利"}
    if pl_ratio >= 50:
        return {"action": "TAKE_PROFIT", "level": "ACT",
                "reason": f"盈利 {pl_ratio:+.1f}%, 可考虑平仓"}
    if days <= 2 and pl_ratio < 0:
        return {"action": "CLOSE_URGENT", "level": "ACT",
                "reason": f"到期仅 {days} 天且亏损, 紧急平仓"}
    if pl_ratio <= -60:
        return {"action": "STOP_LOSS", "level": "ACT",
                "reason": f"亏损 {pl_ratio:+.1f}%, 建议止损"}
    if days <= 5 and pl_ratio < -20:
        return {"action": "STOP_LOSS", "level": "WARN",
                "reason": f"剩 {days} 天+亏损, 建议离场"}
    if days <= 3:
        return {"action": "WATCH_CLOSE", "level": "WARN",
                "reason": f"剩 {days} 天, theta 加速"}
    return {"action": "HOLD", "level": "OK", "reason": "继续持有"}


def detect_straddles(options: list[dict]) -> list[dict]:
    """识别跨式组合: 同 underlying + 同 expiry + 同 strike 的 Call+Put."""
    df = pd.DataFrame(options) if options else pd.DataFrame()
    if df.empty:
        return []
    grouped = df.groupby(["underlying", "expiry", "strike"])
    straddles = []
    for (und, exp, strike), group in grouped:
        if len(group) == 2 and set(group["option_type"]) == {"CALL", "PUT"}:
            call = group[group["option_type"] == "CALL"].iloc[0]
            put = group[group["option_type"] == "PUT"].iloc[0]
            total_cost = (call["cost_price"] + put["cost_price"])
            current_value = (call["current_price"] + put["current_price"])
            pl_per_straddle = (current_value - total_cost) * 100  # 100 股/张
            be_upper = strike + total_cost
            be_lower = strike - total_cost
            theta_daily = (call["theta"] + put["theta"]) * 100 if pd.notna(call.get("theta")) else 0
            straddle = {
                "underlying": und,
                "expiry": exp,
                "strike": strike,
                "spot": call.get("spot", 0),
                "qty": min(call["qty"], put["qty"]),
                "call_code": call["code"],
                "put_code": put["code"],
                "call_cost": call["cost_price"],
                "put_cost": put["cost_price"],
                "call_now": call["current_price"],
                "put_now": put["current_price"],
                "total_cost_per_contract": total_cost,
                "current_value_per_contract": current_value,
                "pl_per_straddle": pl_per_straddle,
                "pl_pct": (current_value / total_cost - 1) * 100 if total_cost else 0,
                "breakeven_upper": be_upper,
                "breakeven_lower": be_lower,
                "days_to_expiry": call.get("days_to_expiry", 0),
                "theta_daily": theta_daily,
                "iv_avg": ((call.get("iv", 0) + put.get("iv", 0)) / 2) if pd.notna(call.get("iv")) else 0,
            }
            # 附上平仓建议
            straddle.update(classify_straddle_action(straddle))
            straddles.append(straddle)
    return straddles


def format_report(options: list[dict], straddles: list[dict]) -> tuple[str, str]:
    """生成完整报告 + 通知摘要."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z")
    lines = []
    lines.append(f"\n{'═' * 88}")
    lines.append(f"  期权持仓监控  {ts}")
    lines.append("═" * 88)

    if not options:
        lines.append("  当前无期权持仓.")
        full = "\n".join(lines)
        return full, "无期权持仓"

    # 识别出的跨式组合
    straddle_codes = set()
    for s in straddles:
        straddle_codes.add(s["call_code"])
        straddle_codes.add(s["put_code"])

    # 独腿期权
    solo = [o for o in options if o["code"] not in straddle_codes]

    if straddles:
        lines.append("\n  [跨式组合 Straddle]")
        for s in straddles:
            # 距盈亏平衡百分比
            spot = s["spot"]
            dist_up = (s["breakeven_upper"] - spot) / spot * 100 if spot else 0
            dist_dn = (spot - s["breakeven_lower"]) / spot * 100 if spot else 0
            pl_sign = "+" if s["pl_per_straddle"] >= 0 else ""
            lines.append(
                f"    {s['underlying']}  strike=${s['strike']:.1f}  到期={s['expiry']} "
                f"({int(s['days_to_expiry'])}天)"
            )
            lines.append(
                f"      现价=${spot:.2f}  "
                f"盈亏平衡: ${s['breakeven_lower']:.2f} (-{dist_dn:.2f}%) | ${s['breakeven_upper']:.2f} (+{dist_up:.2f}%)"
            )
            lines.append(
                f"      总成本 ${s['total_cost_per_contract']:.2f}/张 × {int(s['qty'])}张 = "
                f"${s['total_cost_per_contract']*100*s['qty']:.0f}  "
                f"当前价值 ${s['current_value_per_contract']*100*s['qty']:.0f}"
            )
            lines.append(
                f"      PnL: {pl_sign}${s['pl_per_straddle']*s['qty']:.2f} ({pl_sign}{s['pl_pct']:+.2f}%)  "
                f"Theta日损 ${s['theta_daily']*s['qty']:.2f}  IV均值 {s['iv_avg']:.1f}%"
            )

    if solo:
        lines.append("\n  [独腿期权]")
        for o in solo:
            pl_sign = "+" if (o.get("pl_val") or 0) >= 0 else ""
            lines.append(
                f"    {o['code']}  {o['option_type']} strike=${o['strike']:.1f} "
                f"{int(o.get('days_to_expiry', 0))}天  "
                f"成本 ${o['cost_price']:.2f}/股  现价 ${o['current_price']:.2f}  "
                f"PnL {pl_sign}${(o.get('pl_val') or 0):.2f} ({pl_sign}{(o.get('pl_ratio') or 0)*100:+.2f}%)"
            )
            lines.append(
                f"      Δ={o.get('delta', 0):+.3f} θ={o.get('theta', 0):+.3f} "
                f"ν={o.get('vega', 0):+.3f} IV={o.get('iv', 0):.1f}%"
            )

    lines.append(f"\n{'─' * 88}")

    # 在完整报告里也加"建议动作"行
    if straddles or solo:
        lines.append("\n  [建议动作]")
        for s in straddles:
            action = s.get("action", "HOLD")
            reason = s.get("reason", "")
            marker = "⚠️" if s.get("level") == "ACT" else ("⚡" if s.get("level") == "WARN" else " ")
            lines.append(f"    {marker} {s['underlying']} Straddle: {action} — {reason}")
        for o in solo:
            act = classify_solo_option_action(o)
            marker = "⚠️" if act.get("level") == "ACT" else ("⚡" if act.get("level") == "WARN" else " ")
            lines.append(f"    {marker} {o['code']}: {act['action']} — {act['reason']}")

    # 汇总 (macOS 通知用) — 高优先级动作前置
    summary = []
    urgent_items = []
    normal_items = []
    for s in straddles:
        pl_sign = "+" if s["pl_per_straddle"] >= 0 else ""
        item = (
            f"{s['underlying'].replace('US.','')} Strd @${s['strike']:.0f} "
            f"{int(s['days_to_expiry'])}d {pl_sign}${s['pl_per_straddle']*s['qty']:.0f} ({pl_sign}{s['pl_pct']:+.0f}%)"
        )
        if s.get("level") == "ACT":
            urgent_items.append(f"⚠️ {s.get('action')}: {item}")
        else:
            normal_items.append(item)
    for o in solo:
        act = classify_solo_option_action(o)
        pl_sign = "+" if (o.get("pl_val") or 0) >= 0 else ""
        item = (
            f"{o['underlying'].replace('US.','')} {o['option_type'][0]}${o['strike']:.0f} "
            f"{int(o.get('days_to_expiry', 0))}d {pl_sign}${o.get('pl_val', 0):.0f}"
        )
        if act.get("level") == "ACT":
            urgent_items.append(f"⚠️ {act['action']}: {item}")
        else:
            normal_items.append(item)
    # 紧急排前面, 正常排后面
    summary = urgent_items + normal_items
    notification_text = " | ".join(summary) if summary else "无期权持仓"

    full_report = "\n".join(lines)
    return full_report, notification_text


def send_notification(title: str, message: str) -> None:
    """macOS 通知."""
    # 用 osascript 发送原生通知
    script = f'display notification "{message}" with title "{title}"'
    try:
        subprocess.run(["osascript", "-e", script], timeout=5, check=False)
    except Exception:
        pass


def _compute_realized_pnl() -> tuple[str, str]:
    """从 trade_sim_log.csv 读 OPT_BUY/OPT_SELL 配对, 计算已实现盈亏.
    返回 (金额字符串, 描述).
    """
    log_path = Path(__file__).parent / "trade_sim_log.csv"
    if not log_path.exists():
        return "-", "无交易记录"
    try:
        df = pd.read_csv(log_path)
        df = df[df["action"].isin(["OPT_BUY", "OPT_SELL"])].copy()
        if df.empty:
            return "$0", "无期权交易"

        # 按 code 分组, 匹配 BUY/SELL 配对
        total_pnl = 0.0
        total_trades = 0
        for code, group in df.groupby("code"):
            buys = group[group["action"] == "OPT_BUY"]
            sells = group[group["action"] == "OPT_SELL"]
            # 按时间顺序配对 (FIFO)
            sell_list = [(r["qty"], r["price"]) for _, r in sells.iterrows()]
            buy_list = [(r["qty"], r["price"]) for _, r in buys.iterrows()]
            # 只算"卖出量" × 100 × 价差 (简化)
            total_sell_qty = sum(q for q, _ in sell_list)
            if total_sell_qty == 0:
                continue
            avg_buy = sum(q * p for q, p in buy_list) / sum(q for q, _ in buy_list) if buy_list else 0
            avg_sell = sum(q * p for q, p in sell_list) / total_sell_qty
            pnl = (avg_sell - avg_buy) * 100 * total_sell_qty
            total_pnl += pnl
            total_trades += 1

        sign = "+" if total_pnl >= 0 else ""
        if total_trades == 0:
            return "$0", "持仓未平仓"
        return f"{sign}${total_pnl:.2f}", f"{total_trades} 笔已平仓"
    except Exception:
        return "-", "计算失败"


def _compute_pending_profit(options: list[dict]) -> tuple[str, str]:
    """查 SIMULATE 账户的未成交 SELL 挂单, 计算如果全部成交的潜在盈利."""
    try:
        from futu import OpenSecTradeContext, RET_OK, TrdEnv, TrdMarket, SecurityFirm
        t = OpenSecTradeContext(
            filter_trdmarket=TrdMarket.US, host="127.0.0.1", port=11111,
            security_firm=SecurityFirm.FUTUSECURITIES,
        )
        try:
            ret, orders = t.order_list_query(trd_env=TrdEnv.SIMULATE)
            if ret != RET_OK or orders.empty:
                return "$0", "无挂单"
            active = orders[
                (orders["trd_side"] == "SELL") &
                (orders["order_status"].isin(["SUBMITTED", "SUBMITTING", "WAITING"]))
            ]
            if active.empty:
                return "$0", "无挂单"

            cost_by_code = {o["code"]: o["cost_price"] for o in options}
            total_profit = 0
            for _, r in active.iterrows():
                code = r["code"]
                sell_price = float(r["price"])
                qty = float(r["qty"])
                cost = cost_by_code.get(code)
                if cost is None:
                    continue
                profit_per_contract = (sell_price - cost) * 100
                total_profit += profit_per_contract * qty

            sign = "+" if total_profit >= 0 else ""
            return f"{sign}${total_profit:.0f}", f"{len(active)} 挂单"
        finally:
            t.close()
    except Exception:
        return "-", "查询失败"


def save_html_fragment(options: list[dict], straddles: list[dict], path: str | Path) -> None:
    """生成期权持仓的 HTML 片段 (供 generate_page.py 嵌入主页).

    不输出完整 HTML/CSS, 只输出 <section> 内容, 用主页已有样式呈现.
    """
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    Path(path).parent.mkdir(parents=True, exist_ok=True)

    def _action_tag(level: str, action: str) -> str:
        """根据 action level 渲染 tag HTML."""
        action_cn = {
            "TAKE_PROFIT": "🟢 获利平仓",
            "CLOSE_URGENT": "🔴 紧急平仓",
            "STOP_LOSS": "🔴 止损",
            "CONSIDER_CLOSE": "🟡 考虑平仓",
            "WATCH_CLOSE": "🟡 密切监控",
            "HOLD_CAUTION": "⚪ 警戒持有",
            "HOLD": "⚪ 继续持有",
        }.get(action, action)
        tag_cls = {
            "ACT": "tag tag-up",       # 红色强提示 (行动)
            "WARN": "tag tag-neutral", # 中性 (警告)
            "NOTE": "tag tag-neutral",
            "OK": "tag tag-down",      # 绿色 (OK)
        }.get(level, "tag tag-neutral")
        return f'<span class="{tag_cls}">{action_cn}</span>'

    def _cmd_button(cmd: str, button_label: str) -> str:
        """生成"复制命令"按钮 (单条命令或多条 \\n 分隔)."""
        safe_cmd = cmd.replace('"', '&quot;').replace("'", "\\'")
        return (
            f'<button onclick="navigator.clipboard.writeText(\'{safe_cmd}\')'
            f'.then(()=>{{this.textContent=\'✓ 已复制\';setTimeout(()=>this.textContent=\'{button_label}\',2000)}})" '
            f'style="font-family:inherit;font-size:11px;padding:4px 8px;margin-right:4px;margin-top:4px;'
            f'border:1px solid currentColor;background:var(--paper);cursor:pointer;">{button_label}</button>'
        )

    def _close_cmd_button(codes_qty: list[tuple[str, int]], button_label: str) -> str:
        """生成"复制平仓命令"按钮 (多腿一次复制)."""
        cmds = "\\n".join(
            f"./venv/bin/python trade_futu_sim.py sell {c} {q} --confirm"
            for c, q in codes_qty
        )
        return _cmd_button(cmds, button_label)

    def _limit_gtc_button(codes_qty_price: list[tuple[str, int, float]], label: str) -> str:
        """生成"复制 GTC 止盈挂单命令"按钮."""
        cmds = "\\n".join(
            f"./venv/bin/python trade_futu_sim.py limit_sell {c} {q} {p:.2f} --confirm"
            for c, q, p in codes_qty_price
        )
        return _cmd_button(cmds, label)

    def _stop_button(codes_qty_price: list[tuple[str, int, float]], label: str) -> str:
        """生成"复制 STOP 止损单命令"按钮."""
        cmds = "\\n".join(
            f"./venv/bin/python trade_futu_sim.py stop_sell {c} {q} {p:.2f} --confirm"
            for c, q, p in codes_qty_price
        )
        return _cmd_button(cmds, label)

    # 构造跨式行
    straddle_rows = []
    for s in straddles:
        pl_cls = "up" if s["pl_per_straddle"] >= 0 else "down"
        spot = s["spot"]
        dist_up = (s["breakeven_upper"] - spot) / spot * 100 if spot else 0
        dist_dn = (spot - s["breakeven_lower"]) / spot * 100 if spot else 0
        urgency_tag = ""
        days = int(s["days_to_expiry"])
        if days <= 7:
            urgency_tag = '<span class="tag tag-up">到期近</span>'
        elif days <= 14:
            urgency_tag = '<span class="tag tag-neutral">注意</span>'
        action_tag = _action_tag(s.get("level", "OK"), s.get("action", "HOLD"))
        # 平仓命令按钮 (跨式两腿)
        close_btn = _close_cmd_button(
            [(s['call_code'], int(s['qty'])), (s['put_code'], int(s['qty']))],
            "📋 立即平仓"
        )
        # 止盈挂单: 两腿各按 +30% 挂 GTC 卖单
        call_tp = s['call_cost'] * 1.30
        put_tp = s['put_cost'] * 1.30
        tp_btn = _limit_gtc_button(
            [(s['call_code'], int(s['qty']), call_tp),
             (s['put_code'], int(s['qty']), put_tp)],
            f"🎯 挂止盈 +30%"
        )
        # 止损挂单: 两腿各按 -50% 挂 STOP 单
        call_sl = s['call_cost'] * 0.50
        put_sl = s['put_cost'] * 0.50
        sl_btn = _stop_button(
            [(s['call_code'], int(s['qty']), call_sl),
             (s['put_code'], int(s['qty']), put_sl)],
            f"🛡 挂止损 -50%"
        )
        # 高优先级行高亮整行
        row_style = ' style="background:rgba(203,0,0,0.08);"' if s.get("level") == "ACT" else ""
        # 各腿单独盈亏
        call_pnl = (s['call_now'] - s['call_cost']) * 100 * s['qty']
        put_pnl = (s['put_now'] - s['put_cost']) * 100 * s['qty']
        call_pnl_cls = "up" if call_pnl >= 0 else "down"
        put_pnl_cls = "up" if put_pnl >= 0 else "down"
        call_sign = "+" if call_pnl >= 0 else ""
        put_sign = "+" if put_pnl >= 0 else ""
        legs_pnl_html = (
            f'<small>Call <span class="{call_pnl_cls}">{call_sign}${call_pnl:.0f}</span></small><br>'
            f'<small>Put&nbsp; <span class="{put_pnl_cls}">{put_sign}${put_pnl:.0f}</span></small>'
        )

        straddle_rows.append(f"""
        <tr{row_style}>
          <td><strong>{s['underlying'].replace('US.','')}</strong> 跨式 Straddle</td>
          <td>${s['strike']:.2f}</td>
          <td>{s['expiry']} · {days}天 {urgency_tag}</td>
          <td>${spot:.2f}</td>
          <td><span class="down">↓${s['breakeven_lower']:.2f}</span> / <span class="up">↑${s['breakeven_upper']:.2f}</span><br><small>-{dist_dn:.1f}% / +{dist_up:.1f}%</small></td>
          <td>${s['total_cost_per_contract']*100*s['qty']:.0f}</td>
          <td class="{pl_cls}">{'' if s['pl_per_straddle']<0 else '+'}${s['pl_per_straddle']*s['qty']:.2f}<br><small>({s['pl_pct']:+.2f}%)</small><br>{legs_pnl_html}</td>
          <td>${s['theta_daily']*s['qty']:.2f}</td>
          <td>{s['iv_avg']:.1f}%</td>
          <td>{action_tag}<br><small style="color:var(--muted);">{s.get('reason', '')}</small><br>{close_btn}{tp_btn}{sl_btn}</td>
        </tr>""")

    straddle_codes = {s['call_code'] for s in straddles} | {s['put_code'] for s in straddles}
    solo_rows = []
    for o in options:
        if o['code'] in straddle_codes:
            continue
        pl_val = o.get("pl_val") or 0
        pl_ratio = (o.get("pl_ratio") or 0) * 100
        pl_cls = "up" if pl_val >= 0 else "down"
        days = int(o.get("days_to_expiry", 0))
        urgency_tag = '<span class="tag tag-up">到期近</span>' if days <= 7 else ""
        solo_act = classify_solo_option_action(o)
        solo_tag_html = _action_tag(solo_act["level"], solo_act["action"])
        solo_close_btn = _close_cmd_button([(o['code'], int(o['qty']))], "📋 立即平仓")
        solo_tp = o['cost_price'] * 1.30
        solo_sl = o['cost_price'] * 0.50
        solo_tp_btn = _limit_gtc_button([(o['code'], int(o['qty']), solo_tp)], "🎯 挂止盈 +30%")
        solo_sl_btn = _stop_button([(o['code'], int(o['qty']), solo_sl)], "🛡 挂止损 -50%")
        solo_row_style = ' style="background:rgba(203,0,0,0.08);"' if solo_act.get("level") == "ACT" else ""
        solo_rows.append(f"""
        <tr{solo_row_style}>
          <td><strong>{o['underlying'].replace('US.','')}</strong> {o['option_type']}</td>
          <td>${o['strike']:.2f}</td>
          <td>{o['expiry']} · {days}天 {urgency_tag}</td>
          <td>${o.get('spot', 0):.2f}</td>
          <td>Δ {o.get('delta', 0):+.3f} · θ {o.get('theta', 0):+.3f}</td>
          <td>${o['cost_price']*100:.0f}</td>
          <td class="{pl_cls}">{'' if pl_val<0 else '+'}${pl_val:.2f}<br><small>({pl_ratio:+.2f}%)</small></td>
          <td>${(o.get('theta', 0))*100:.2f}</td>
          <td>{o.get('iv', 0):.1f}%</td>
          <td>{solo_tag_html}<br><small style="color:var(--muted);">{solo_act.get('reason', '')}</small><br>{solo_close_btn}{solo_tp_btn}{solo_sl_btn}</td>
        </tr>""")

    has_content = bool(straddle_rows or solo_rows)

    # 识别紧急 (ACT 级) 期权, 用于页首横幅
    urgent_items = [s for s in straddles if s.get("level") == "ACT"]
    for o in options:
        if o["code"] in straddle_codes:
            continue
        if classify_solo_option_action(o).get("level") == "ACT":
            urgent_items.append({"underlying": o["underlying"], "action": classify_solo_option_action(o).get("action")})

    # 输出 HTML fragment (不含 <html>/<body>, 使用主页已有样式类)
    parts = []
    parts.append(f'<!-- 期权持仓 section, 由 option_monitor.py 生成于 {ts} -->')

    # 紧急横幅 + 浏览器标题动态 (仅有 ACT 级别建议时显示)
    if urgent_items:
        names = ", ".join(
            f"{u.get('underlying','?').replace('US.','')} {u.get('action','')}"
            for u in urgent_items
        )
        parts.append(f"""
<div style="background:#cf222e;color:#fff;padding:14px 20px;margin:20px 0;
            border-radius:4px;font-weight:600;font-family:'JetBrains Mono',monospace;
            letter-spacing:0.05em;">
  ⚠️ 期权平仓提示: {names} &mdash; 请滚动到 № 09 节查看建议动作
</div>
<script>
  // 动态修改浏览器标签页标题, 有紧急建议时闪烁
  (function(){{
    var origTitle = document.title;
    var flashTitles = ['⚠️ 期权注意', origTitle];
    var i = 0;
    setInterval(function(){{ document.title = flashTitles[i++ % 2]; }}, 1500);
  }})();
</script>""")

    parts.append('<section class="section">')
    parts.append('  <div class="section-head">')
    parts.append('    <div class="section-num">№ 09</div>')
    parts.append('    <h2><em>Option</em> Positions<span class="cn">期权持仓</span></h2>')
    parts.append(f'    <div class="section-meta">模拟盘 SIMULATE<br>更新 {ts.split()[1]}</div>')
    parts.append('  </div>')

    # 账户汇总栏 (所有期权持仓合计)
    if options:
        total_cost = sum(o["cost_price"] * 100 * o["qty"] for o in options)
        total_value = sum(o["current_price"] * 100 * o["qty"] for o in options)
        total_pnl = total_value - total_cost
        total_pnl_pct = (total_pnl / total_cost * 100) if total_cost else 0
        pnl_color = "var(--up)" if total_pnl >= 0 else "var(--down)"
        pnl_sign = "+" if total_pnl >= 0 else ""
        n_legs = len(options)

        # 累计已实现盈亏 (从 trade_sim_log.csv 读 OPT_BUY/OPT_SELL 配对)
        realized_pnl, realized_note = _compute_realized_pnl()

        # 止盈挂单潜在盈利 (如全部成交)
        pending_profit, pending_note = _compute_pending_profit(options)

        parts.append(f"""
  <div style="margin-left:128px; margin-bottom:20px; padding:14px 18px;
              background:var(--paper-2); border-left:3px solid {pnl_color};
              font-family:'JetBrains Mono', monospace; font-size:13px;">
    <div style="font-size:11px; letter-spacing:0.12em; color:var(--muted); text-transform:uppercase; margin-bottom:6px;">
      📊 账户汇总 · Options
    </div>
    <div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(180px, 1fr)); gap:12px;">
      <div>
        <div style="color:var(--muted); font-size:10px;">总持仓成本</div>
        <div style="font-size:16px; font-weight:600;">${total_cost:,.0f}</div>
        <div style="color:var(--muted); font-size:10px;">{n_legs} 腿期权</div>
      </div>
      <div>
        <div style="color:var(--muted); font-size:10px;">当前市值</div>
        <div style="font-size:16px; font-weight:600;">${total_value:,.0f}</div>
      </div>
      <div>
        <div style="color:var(--muted); font-size:10px;">浮动盈亏</div>
        <div style="font-size:18px; font-weight:700; color:{pnl_color};">
          {pnl_sign}${total_pnl:,.2f}
        </div>
        <div style="color:{pnl_color}; font-size:11px;">{pnl_sign}{total_pnl_pct:+.2f}%</div>
      </div>
      <div>
        <div style="color:var(--muted); font-size:10px;">累计已实现</div>
        <div style="font-size:14px; font-weight:600;">{realized_pnl}</div>
        <div style="color:var(--muted); font-size:10px;">{realized_note}</div>
      </div>
      <div>
        <div style="color:var(--muted); font-size:10px;">挂单潜在盈利</div>
        <div style="font-size:14px; font-weight:600; color:var(--up);">{pending_profit}</div>
        <div style="color:var(--muted); font-size:10px;">{pending_note}</div>
      </div>
    </div>
  </div>""")

    parts.append('  <p class="note">期权持仓实时监控 · 每小时由 launchd 自动重算 · 点"📋 复制平仓命令"复制到终端执行</p>')
    if not has_content:
        parts.append('  <div style="margin-left:128px; color:var(--muted); padding:20px;">当前无期权持仓</div>')
    else:
        if straddle_rows:
            parts.append('  <div class="table-wrap" style="margin-bottom:24px;">')
            parts.append('  <table>')
            parts.append('  <thead><tr><th>组合</th><th>行权价</th><th>到期</th><th>现价</th><th>盈亏平衡</th><th>成本</th><th>PnL</th><th>Theta/天</th><th>IV</th><th>建议动作</th></tr></thead>')
            parts.append('  <tbody>')
            parts.extend(straddle_rows)
            parts.append('  </tbody></table></div>')
        if solo_rows:
            parts.append('  <div class="table-wrap">')
            parts.append('  <table>')
            parts.append('  <thead><tr><th>合约</th><th>行权价</th><th>到期</th><th>现价</th><th>Greeks</th><th>成本</th><th>PnL</th><th>Theta/天</th><th>IV</th><th>建议动作</th></tr></thead>')
            parts.append('  <tbody>')
            parts.extend(solo_rows)
            parts.append('  </tbody></table></div>')
    parts.append('</section>')

    Path(path).write_text('\n'.join(parts), encoding="utf-8")


def run(quiet: bool = False, trd_env: str = "SIMULATE",
        market_filter: bool = False, html_output: str | None = None) -> None:
    # 如果启用市场过滤, 仅在美股盘中/盘前/盘后运行
    if market_filter:
        from fetch_futu import health_check
        hc = health_check()
        state = hc.get("market_us", "REST")
        if state not in ("MORNING", "AFTERNOON", "PRE_MARKET", "AFTER_HOURS"):
            # 美股休市, 不跑
            return

    options = analyze_positions(trd_env=trd_env)
    straddles = detect_straddles(options)
    report, summary = format_report(options, straddles)

    # 写日志 (累积)
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(report + "\n")

    # 写最新状态 (覆盖)
    with LOG_TEXT_PATH.open("w", encoding="utf-8") as f:
        f.write(report)

    # 控制台输出
    print(report)

    # macOS 通知
    if not quiet and options:
        send_notification("期权持仓", summary[:200])

    # HTML 片段输出 (默认写到 option_section.html, 供 generate_page.py 嵌入主页)
    if html_output is None:
        html_output = str(Path(__file__).parent / "option_section.html")
    try:
        save_html_fragment(options, straddles, html_output)
    except Exception as e:
        print(f"  [!] HTML 片段保存失败: {e}")


if __name__ == "__main__":
    quiet = "--quiet" in sys.argv
    trd_env = "REAL" if "--real" in sys.argv else "SIMULATE"
    market_filter = "--market-open-only" in sys.argv
    run(quiet=quiet, trd_env=trd_env, market_filter=market_filter)
