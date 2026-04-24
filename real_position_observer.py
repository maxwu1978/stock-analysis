"""真实盘持仓观察器 — 纯只读, 绝不下单或给交易建议

功能:
  1. 读 Futu 真实账户持仓 (REAL 模式)
  2. 对每只持仓股计算分形谱 + 技术指标
  3. 输出 HTML 片段 real_position_section.html, 供 generate_page.py 嵌入主页 № 10 节
  4. 不包含 "建议平仓/加仓" 语言, 只给客观信号描述 (asym/hq2/RSI/MA偏离)

原则:
  - 永远不调用 place_order / unlock_trade
  - 语言中性: 描述 "当前状态" 而非 "建议动作"
  - 风险提示用 "参考" / "观察" 字样, 不用 "应该/建议"
  - 退出模板接在 option_monitor.py 的期权监控页；本文件仅明确真实盘股票观察页不适用
"""

import argparse
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from fetch_futu import get_positions, realtime_quotes, get_kline
from fractal_survey import mfdfa_spectrum


def describe_fractal_state(asym: float, hq2: float) -> str:
    """客观描述分形状态, 不含 "建议" 字样."""
    if pd.isna(asym) or pd.isna(hq2):
        return "数据不足"
    tags = []
    if asym > 0.3:
        tags.append("强非对称")
    elif asym > 0.1:
        tags.append("显著非对称")
    elif asym < -0.1:
        tags.append("反向非对称")
    else:
        tags.append("弱/中性")
    if hq2 > 0.55:
        tags.append("持续性")
    elif hq2 < 0.45:
        tags.append("反持续")
    return " · ".join(tags)


def describe_technical_state(rsi6: float, ma20_diff: float) -> str:
    """客观描述技术面."""
    tags = []
    if rsi6 >= 80:
        tags.append("极度超买")
    elif rsi6 >= 70:
        tags.append("超买")
    elif rsi6 <= 20:
        tags.append("极度超卖")
    elif rsi6 <= 30:
        tags.append("超卖")
    if ma20_diff > 10:
        tags.append("远离均线")
    elif ma20_diff < -10:
        tags.append("深度跌破")
    return " · ".join(tags) if tags else "技术中性"


def analyze_stock(code: str, qty: float, cost_price: float) -> dict:
    """对单只股票分析. 只观察, 不建议."""
    out = {"code": code, "qty": qty, "cost_price": cost_price}

    # 实时价
    try:
        rt = realtime_quotes([code])
        out["current_price"] = float(rt.iloc[0]["last_price"])
        out["chg_today_pct"] = float(rt.iloc[0]["change_rate"])
    except Exception as e:
        out["error"] = f"实时价失败: {str(e)[:50]}"
        return out

    # 持仓盈亏
    out["market_value"] = out["current_price"] * qty
    out["unrealized_pnl"] = (out["current_price"] - cost_price) * qty
    out["unrealized_pnl_pct"] = (out["current_price"] / cost_price - 1) * 100

    # 拉历史算分形 + 技术
    try:
        kl = get_kline(code, days=200, ktype="K_DAY")
        closes = kl["close"].astype(float)
        log_ret = np.log(closes / closes.shift(1))

        if len(log_ret) >= 120:
            spec = mfdfa_spectrum(log_ret.iloc[-120:])
            out.update(spec)

        # 技术指标
        ma20 = closes.rolling(20).mean().iloc[-1]
        out["ma20_diff_pct"] = (closes.iloc[-1] / ma20 - 1) * 100 if pd.notna(ma20) else 0
        out["vol_20d_ann"] = log_ret.iloc[-20:].std() * np.sqrt(252) * 100

        delta = closes.diff()
        gain = delta.where(delta > 0, 0).rolling(6).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(6).mean()
        rs = gain / loss
        out["rsi6"] = float((100 - 100 / (1 + rs)).iloc[-1])
    except Exception as e:
        out["error"] = f"历史/分形失败: {str(e)[:50]}"

    return out


def save_html_fragment(positions: list[dict], path: str | Path) -> None:
    """生成真实盘观察的 HTML 片段供主页嵌入."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    Path(path).parent.mkdir(parents=True, exist_ok=True)

    rows = []
    for p in positions:
        if "error" in p:
            rows.append(f"""
        <tr>
          <td><strong>{p['code'].replace('US.','')}</strong></td>
          <td colspan="9" style="color:var(--muted);">{p['error']}</td>
        </tr>""")
            continue

        pnl = p.get("unrealized_pnl", 0)
        pnl_cls = "up" if pnl >= 0 else "down"
        pnl_sign = "+" if pnl >= 0 else ""
        chg = p.get("chg_today_pct", 0)
        chg_cls = "up" if chg >= 0 else "down"

        fractal_desc = describe_fractal_state(p.get("asym"), p.get("hq2"))
        tech_desc = describe_technical_state(p.get("rsi6", 50), p.get("ma20_diff_pct", 0))

        rows.append(f"""
        <tr>
          <td><strong>{p['code'].replace('US.','')}</strong></td>
          <td>{p['qty']:.2f} 股</td>
          <td>${p['cost_price']:.2f}</td>
          <td>${p.get('current_price', 0):.2f}<br><small class="{chg_cls}">今日 {chg:+.2f}%</small></td>
          <td>${p.get('market_value', 0):,.0f}</td>
          <td class="{pnl_cls}">{pnl_sign}${pnl:.0f}<br><small>({pnl_sign}{p.get('unrealized_pnl_pct', 0):.2f}%)</small></td>
          <td>asym {p.get('asym', 0):+.2f}<br><small>{fractal_desc}</small></td>
          <td>RSI6 {p.get('rsi6', 0):.0f}<br><small>{tech_desc}</small></td>
          <td>{p.get('vol_20d_ann', 0):.0f}%</td>
        </tr>""")

    if not rows:
        rows.append('<tr><td colspan="9" class="empty" style="color:var(--muted); padding:20px;">真实账户无持仓或查询失败</td></tr>')

    parts = [
        f'<!-- 真实盘观察 section, 由 real_position_observer.py 生成于 {ts} -->',
        '<section class="section">',
        '  <div class="section-head">',
        '    <div class="section-num">№ 10</div>',
        '    <h2><em>Real Account</em><span class="cn">真实盘观察</span></h2>',
        f'    <div class="section-meta">真实账户 REAL<br>{ts.split()[1]} · 只读</div>',
        '  </div>',
        '  <p class="note">⚠ 仅观察描述当前分形/技术状态, 不含交易建议 · 股票观察页不适用期权退出模板；期权模板状态见 № 09 节</p>',
        '  <div class="table-wrap">',
        '  <table>',
        '  <thead><tr><th>标的</th><th>持仓</th><th>成本</th><th>现价/涨跌</th><th>市值</th><th>浮动盈亏</th><th>分形状态</th><th>技术状态</th><th>年化σ</th></tr></thead>',
        '  <tbody>',
    ]
    parts.extend(rows)
    parts.extend([
        '  </tbody></table></div>',
        '</section>',
    ])

    Path(path).write_text('\n'.join(parts), encoding="utf-8")


def run(output_html: str | None = None) -> list[dict]:
    """拉真实盘持仓 + 分析 + 生成 HTML 片段."""
    print("[1/3] 拉真实账户持仓 (只读)...")
    pos = get_positions(trd_env="REAL")
    if pos.empty:
        print("  无持仓")
        positions = []
    else:
        # 只看股票 (期权代号长+含C/P)
        is_stock = ~pos["code"].str.contains(r"\d{6}[CP]\d+$", regex=True, na=False)
        stocks = pos[is_stock & (pos["qty"] > 0)] if "qty" in pos.columns else pos[is_stock]
        print(f"  找到 {len(stocks)} 只股票持仓")

        positions = []
        for _, r in stocks.iterrows():
            print(f"  分析 {r['code']}...", end="", flush=True)
            p = analyze_stock(r["code"], float(r["qty"]), float(r["cost_price"]))
            positions.append(p)
            print(" 完成")

    print("[2/3] 生成 HTML 片段...")
    if output_html is None:
        output_html = str(Path(__file__).parent / "real_position_section.html")
    save_html_fragment(positions, output_html)
    print(f"  保存: {output_html}")

    print("[3/3] 观察摘要")
    print()
    print("  注: 退出模板已接入 №09 期权监控页；本节真实盘股票观察不应用期权模板。")
    for p in positions:
        if "error" in p:
            print(f"  {p['code']}: {p['error']}")
            continue
        print(
            f"  {p['code'].replace('US.',''):<8} "
            f"{p['qty']:>7.2f}股  成本 ${p['cost_price']:>7.2f}  现价 ${p.get('current_price', 0):>7.2f}  "
            f"PnL ${p.get('unrealized_pnl', 0):>+8.0f} ({p.get('unrealized_pnl_pct', 0):>+.2f}%)  "
            f"asym {p.get('asym', 0):>+.2f}  RSI {p.get('rsi6', 0):>.0f}"
        )

    return positions


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read REAL account positions and emit a read-only HTML fragment")
    parser.add_argument("--output-html", help="real-position section fragment output path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(output_html=args.output_html)


if __name__ == "__main__":
    main()
