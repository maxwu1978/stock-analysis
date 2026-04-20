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
            straddles.append({
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
            })
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

    # 汇总 (macOS 通知用)
    summary = []
    for s in straddles:
        pl_sign = "+" if s["pl_per_straddle"] >= 0 else ""
        summary.append(
            f"{s['underlying']} Straddle @${s['strike']:.0f} "
            f"剩{int(s['days_to_expiry'])}天  {pl_sign}${s['pl_per_straddle']*s['qty']:.0f} ({pl_sign}{s['pl_pct']:+.1f}%)"
        )
    for o in solo:
        pl_sign = "+" if (o.get("pl_val") or 0) >= 0 else ""
        summary.append(
            f"{o['underlying']} {o['option_type'][0]} ${o['strike']:.0f} "
            f"剩{int(o.get('days_to_expiry', 0))}天  {pl_sign}${o.get('pl_val', 0):.0f}"
        )
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


def save_html_fragment(options: list[dict], straddles: list[dict], path: str | Path) -> None:
    """生成期权持仓的 HTML 片段 (供 generate_page.py 嵌入主页).

    不输出完整 HTML/CSS, 只输出 <section> 内容, 用主页已有样式呈现.
    """
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    Path(path).parent.mkdir(parents=True, exist_ok=True)

    # 构造跨式行 (使用主页站点的 .up/.down/.strong/.weak 类名复用样式)
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
        straddle_rows.append(f"""
        <tr>
          <td><strong>{s['underlying'].replace('US.','')}</strong> 跨式 Straddle</td>
          <td>${s['strike']:.2f}</td>
          <td>{s['expiry']} · {days}天 {urgency_tag}</td>
          <td>${spot:.2f}</td>
          <td><span class="down">↓${s['breakeven_lower']:.2f}</span> / <span class="up">↑${s['breakeven_upper']:.2f}</span><br><small>-{dist_dn:.1f}% / +{dist_up:.1f}%</small></td>
          <td>${s['total_cost_per_contract']*100*s['qty']:.0f}</td>
          <td class="{pl_cls}">{'' if s['pl_per_straddle']<0 else '+'}${s['pl_per_straddle']*s['qty']:.2f}<br><small>({s['pl_pct']:+.2f}%)</small></td>
          <td>${s['theta_daily']*s['qty']:.2f}</td>
          <td>{s['iv_avg']:.1f}%</td>
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
        solo_rows.append(f"""
        <tr>
          <td><strong>{o['underlying'].replace('US.','')}</strong> {o['option_type']}</td>
          <td>${o['strike']:.2f}</td>
          <td>{o['expiry']} · {days}天 {urgency_tag}</td>
          <td>${o.get('spot', 0):.2f}</td>
          <td>Δ {o.get('delta', 0):+.3f} · θ {o.get('theta', 0):+.3f}</td>
          <td>${o['cost_price']*100:.0f}</td>
          <td class="{pl_cls}">{'' if pl_val<0 else '+'}${pl_val:.2f}<br><small>({pl_ratio:+.2f}%)</small></td>
          <td>${(o.get('theta', 0))*100:.2f}</td>
          <td>{o.get('iv', 0):.1f}%</td>
        </tr>""")

    has_content = bool(straddle_rows or solo_rows)

    # 输出 HTML fragment (不含 <html>/<body>, 使用主页已有样式类)
    parts = []
    parts.append(f'<!-- 期权持仓 section, 由 option_monitor.py 生成于 {ts} -->')
    parts.append('<section class="section">')
    parts.append('  <div class="section-head">')
    parts.append('    <div class="section-num">№ 09</div>')
    parts.append('    <h2><em>Option</em> Positions<span class="cn">期权持仓</span></h2>')
    parts.append(f'    <div class="section-meta">模拟盘 SIMULATE<br>更新 {ts.split()[1]}</div>')
    parts.append('  </div>')
    parts.append('  <p class="note">期权持仓实时监控 · 每小时由 launchd 自动重算 · 仅限模拟盘</p>')
    if not has_content:
        parts.append('  <div style="margin-left:128px; color:var(--muted); padding:20px;">当前无期权持仓</div>')
    else:
        if straddle_rows:
            parts.append('  <div class="table-wrap" style="margin-bottom:24px;">')
            parts.append('  <table>')
            parts.append('  <thead><tr><th>组合</th><th>行权价</th><th>到期</th><th>现价</th><th>盈亏平衡</th><th>成本</th><th>PnL</th><th>Theta/天</th><th>IV</th></tr></thead>')
            parts.append('  <tbody>')
            parts.extend(straddle_rows)
            parts.append('  </tbody></table></div>')
        if solo_rows:
            parts.append('  <div class="table-wrap">')
            parts.append('  <table>')
            parts.append('  <thead><tr><th>合约</th><th>行权价</th><th>到期</th><th>现价</th><th>Greeks</th><th>成本</th><th>PnL</th><th>Theta/天</th><th>IV</th></tr></thead>')
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
