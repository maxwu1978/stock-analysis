#!/usr/bin/env python3
"""生成静态HTML页面, 用于GitHub Pages部署"""

import argparse
import os
import sys
from datetime import datetime
import pandas as pd

from fetch_data import STOCKS, fetch_realtime_quotes, fetch_all_history
from indicators import compute_all, summarize
from probability import score_trend
from probability_us import score_trend_us
from fundamental import fetch_all_financials
from fetch_us import US_STOCKS, fetch_us_realtime, fetch_us_all_history, fetch_us_financials

A_SIGNAL_RELIABILITY = {"300750": "强", "600519": "弱", "601600": "强", "300274": "弱", "600745": "中"}
US_SIGNAL_RELIABILITY = {"NVDA": "中", "TSLA": "强", "GOOGL": "弱", "AAPL": "中", "TCOM": "?", "FUTU": "?"}


def ensure_complete_dataset(all_hist: dict, label: str, expected: dict) -> None:
    """关键标的缺失时直接失败，避免发布残缺页面。"""
    missing = [f"{name}({code})" for code, name in expected.items() if code not in all_hist]
    if missing:
        raise RuntimeError(f"{label}历史数据缺失: {', '.join(missing)}")


def parse_args():
    parser = argparse.ArgumentParser(description="生成 GitHub Pages 静态页面")
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="允许部分标的缺失，仅用于本地预览；默认严格模式会直接失败",
    )
    return parser.parse_args()


def direction_tag(hp):
    p30 = hp.get("30日")
    if not p30:
        return '<span class="tag tag-neutral">--</span>'
    pct = int(p30["上涨概率"].replace("%", ""))
    if pct >= 60:
        return '<span class="tag tag-up">看涨</span>'
    if pct >= 55:
        return '<span class="tag tag-up">偏涨</span>'
    if pct <= 35:
        return '<span class="tag tag-down">看跌</span>'
    if pct <= 45:
        return '<span class="tag tag-down">偏跌</span>'
    return '<span class="tag tag-neutral">震荡</span>'


def fmt_prob_cell(d):
    if not d:
        return '<td>-</td>'
    p = int(d["上涨概率"].replace("%", ""))
    avg = d["平均收益"]
    n = d["样本数"]
    if p >= 60:
        return f'<td class="strong">{p}% {avg} <small>(n={n})</small></td>'
    if p <= 40:
        return f'<td class="weak">{p}% {avg} <small>(n={n})</small></td>'
    return f'<td>{p}% {avg} <small>(n={n})</small></td>'


def chg_td(val):
    if not isinstance(val, (int, float)):
        return f'<td>{val}</td>'
    sign = "+" if val >= 0 else ""
    cls = "up" if val >= 0 else "down"
    return f'<td class="{cls}">{sign}{val:.2f}%</td>'


def generate(allow_partial: bool = False):
    print("获取实时行情...")
    try:
        rt = fetch_realtime_quotes()
    except Exception as e:
        print(f"  [!] {e}")
        rt = pd.DataFrame()

    print("获取财报数据...")
    try:
        all_fund = fetch_all_financials()
    except Exception as e:
        print(f"  [!] {e}")
        all_fund = {}

    print("获取历史数据...")
    try:
        all_hist = fetch_all_history(days=800)
    except Exception as e:
        print(f"  [!] {e}")
        all_hist = {}
    if allow_partial:
        missing = [name for code, name in STOCKS.items() if code not in all_hist]
        if missing:
            print(f"  [!] A股历史数据缺失，继续本地预览: {', '.join(missing)}")
    else:
        ensure_complete_dataset(all_hist, "A股", STOCKS)

    # 行情
    quote_html = ""
    if not rt.empty:
        for _, r in rt.iterrows():
            chg = r.get("涨跌幅", 0)
            amt = r.get("成交额", 0)
            amt_s = f"{amt/1e8:.1f}亿" if amt >= 1e8 else "-"
            quote_html += f'<tr><td>{r["名称"]}</td><td>{r["代码"]}</td>'
            quote_html += f'<td>{r.get("最新价",0):.2f}</td>'
            quote_html += chg_td(chg)
            quote_html += f'<td>{amt_s}</td>'
            quote_html += f'<td>{r.get("最高",0):.2f}</td>'
            quote_html += f'<td>{r.get("最低",0):.2f}</td></tr>\n'

    # 概率 + 技术 + 财报
    prob_html = ""
    tech_html = ""
    fund_html = ""

    for code, df in all_hist.items():
        name = STOCKS[code]
        fund_df = all_fund.get(code)
        df = compute_all(df, fund_df)
        s = summarize(df)
        if not s:
            continue
        prob = score_trend(df)
        if "error" in prob:
            continue
        hp = prob.get("historical_prob", {})
        rg = prob.get("regime", {})

        reliability = A_SIGNAL_RELIABILITY.get(code, "?")
        rel_cls = "strong" if reliability == "强" else ("weak" if reliability == "弱" else "")

        ft_score = df["fat_tail_score"].iloc[-1] if "fat_tail_score" in df.columns else 0
        if pd.isna(ft_score): ft_score = 0
        ft_score = int(ft_score)
        ft_html = f'<td class="strong">{"⚡" * ft_score}</td>' if ft_score >= 3 else '<td>-</td>'

        prob_html += f'<tr><td>{name}</td><td>{direction_tag(hp)}</td><td class="{rel_cls}">{reliability}</td>{ft_html}'
        for period in ["5日", "10日", "30日", "180日"]:
            prob_html += fmt_prob_cell(hp.get(period))
        prob_html += '</tr>\n'

        stype = {"momentum": "动量", "mean_revert": "回归", "mixed": "混合"}.get(rg.get("stock_type"), "?")
        try:
            rsi_v = float(s["RSI6"])
            rsi_cls = "up" if rsi_v > 60 else ("down" if rsi_v < 40 else "")
        except:
            rsi_cls = ""
        tech_html += f'<tr><td>{name}</td>'
        tech_html += f'<td class="{rsi_cls}">{s["RSI6"]}</td>'
        tech_html += f'<td>{s["MACD"]}</td>'
        tech_html += f'<td>{s["MA5"]}</td><td>{s["MA20"]}</td><td>{s["MA60"]}</td>'
        tech_html += f'<td>{rg.get("adx",0):.0f}</td><td>{stype}</td></tr>\n'

        if fund_df is not None and not fund_df.empty:
            lt = fund_df.iloc[-1]
            rpt = lt.get("report_date")
            rpt_s = rpt.strftime("%Y-%m-%d") if hasattr(rpt, "strftime") else str(rpt)
            def fv(key, fmt=".1f"):
                v = lt.get(key)
                if pd.isna(v): return "-"
                return f"{v:{fmt}}%"
            def fv_td(key, fmt=".1f", invert=False):
                v = lt.get(key)
                if pd.isna(v): return '<td>-</td>'
                s_val = f"{v:{fmt}}%"
                if invert:
                    cls = "down" if v > 60 else ("up" if v < 30 else "")
                else:
                    cls = "up" if v > 15 else ("down" if v < 0 else "")
                return f'<td class="{cls}">{s_val}</td>'
            fund_html += f'<tr><td>{name}</td><td>{rpt_s}</td>'
            fund_html += fv_td("roe")
            fund_html += fv_td("rev_growth", "+.1f")
            fund_html += fv_td("profit_growth", "+.1f")
            fund_html += f'<td>{fv("gross_margin")}</td>'
            fund_html += fv_td("debt_ratio", ".1f", invert=True)
            fund_html += '</tr>\n'

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>主力分析 · QUANT DESK</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=Spectral:ital,wght@0,300;0,400;0,500;1,400&family=JetBrains+Mono:wght@400;500;600&family=Noto+Serif+SC:wght@400;500;700&display=swap" rel="stylesheet">
<style>
  :root {{
    --ink: #141211;
    --paper: #f2ede2;
    --paper-2: #e8dfcd;
    --up: #b8251f;
    --down: #2a5f4a;
    --muted: #726b61;
    --hair: #c9c0ae;
  }}
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  html, body {{ background: var(--paper); }}
  body {{
    font-family: 'Spectral', 'Noto Serif SC', Georgia, serif;
    color: var(--ink);
    font-feature-settings: "lnum", "tnum";
    -webkit-font-smoothing: antialiased;
    line-height: 1.5;
    position: relative;
    overflow-x: hidden;
  }}
  body::before {{
    content: '';
    position: fixed; inset: 0;
    background-image: url("data:image/svg+xml,%3Csvg viewBox='0 0 200 200' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='3'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)' opacity='0.55'/%3E%3C/svg%3E");
    opacity: 0.04; pointer-events: none; z-index: 200; mix-blend-mode: multiply;
  }}
  .tape {{
    border-bottom: 1px solid var(--ink);
    padding: 9px 28px;
    font-family: 'JetBrains Mono', 'PingFang SC', monospace;
    font-size: 10.5px; letter-spacing: 0.22em; text-transform: uppercase;
    display: flex; justify-content: space-between; gap: 16px; flex-wrap: wrap;
    position: sticky; top: 0; background: var(--paper); z-index: 10;
  }}
  .tape .dot {{ color: var(--up); animation: pulse 2s ease-in-out infinite; }}
  .tape .muted {{ color: var(--muted); }}
  @keyframes pulse {{ 0%, 100% {{ opacity: 1; }} 50% {{ opacity: 0.35; }} }}
  .container {{ max-width: 1180px; margin: 0 auto; padding: 0 28px 80px; }}
  .hero {{ padding: 72px 0 56px; border-bottom: 2px solid var(--ink); }}
  .hero-kicker {{
    display: flex; gap: 16px; align-items: center; margin-bottom: 28px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px; letter-spacing: 0.24em; text-transform: uppercase;
    color: var(--muted);
  }}
  .hero-kicker::before {{ content: ''; width: 48px; height: 1px; background: var(--ink); display: inline-block; }}
  .hero h1 {{
    font-family: 'DM Serif Display', 'Noto Serif SC', serif;
    font-weight: 400;
    font-size: clamp(56px, 10vw, 148px);
    line-height: 0.88;
    letter-spacing: -0.028em;
  }}
  .hero h1 em {{ font-style: italic; color: var(--up); }}
  .hero h1 .eyebrow {{
    display: block; font-size: 0.14em; letter-spacing: 0.26em;
    text-transform: uppercase; color: var(--muted);
    font-family: 'JetBrains Mono', monospace; font-style: normal;
    margin-top: 28px; font-weight: 400;
  }}
  .hero-meta {{
    margin-top: 44px;
    display: flex; flex-wrap: wrap; align-items: center; gap: 18px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px; letter-spacing: 0.1em; color: var(--muted);
  }}
  .pill {{
    border: 1px solid var(--ink); padding: 7px 13px;
    text-transform: uppercase; color: var(--ink);
    font-size: 10.5px; letter-spacing: 0.16em;
  }}
  .btn-refresh {{
    background: var(--ink); color: var(--paper); border: 1px solid var(--ink);
    padding: 9px 18px; font-family: 'JetBrains Mono', monospace;
    font-size: 10.5px; letter-spacing: 0.2em; text-transform: uppercase;
    cursor: pointer; transition: transform 0.18s, background 0.18s;
  }}
  .btn-refresh:hover:not(:disabled) {{ background: var(--up); border-color: var(--up); transform: translateY(-1px); }}
  .btn-refresh:disabled {{ opacity: 0.45; cursor: wait; }}
  .refresh-msg {{
    font-family: 'JetBrains Mono', monospace; font-size: 10.5px;
    letter-spacing: 0.12em; text-transform: uppercase; color: var(--muted);
  }}
  .section {{ padding: 56px 0 32px; }}
  .section + .section {{ border-top: 1px solid var(--hair); }}
  .section-head {{
    display: grid; grid-template-columns: 100px 1fr auto;
    gap: 28px; align-items: start; margin-bottom: 28px;
  }}
  .section-num {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px; letter-spacing: 0.22em; color: var(--muted);
    border-top: 2px solid var(--ink); padding-top: 12px;
  }}
  .section-head h2 {{
    font-family: 'DM Serif Display', 'Noto Serif SC', serif;
    font-weight: 400; font-size: clamp(32px, 4.4vw, 54px);
    line-height: 1; letter-spacing: -0.018em;
    border-top: 2px solid var(--ink); padding-top: 4px; color: var(--ink);
  }}
  .section-head h2 em {{ font-style: italic; color: var(--up); }}
  .section-head h2 .cn {{
    font-size: 0.36em; font-family: 'Noto Serif SC', serif;
    color: var(--muted); letter-spacing: 0.02em; margin-left: 16px;
    font-style: normal; font-weight: 400; vertical-align: 0.15em;
  }}
  .section-meta {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 10.5px; letter-spacing: 0.16em; text-transform: uppercase;
    color: var(--muted); border-top: 1px solid var(--hair);
    padding-top: 12px; text-align: right; max-width: 200px; line-height: 1.8;
  }}
  .note {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 10.5px; letter-spacing: 0.12em; color: var(--muted);
    margin: 8px 0 20px 128px; text-transform: uppercase;
  }}
  .table-wrap {{ overflow-x: auto; margin-left: 128px; }}
  @media (max-width: 820px) {{
    .section-head {{ grid-template-columns: 1fr; gap: 10px; }}
    .table-wrap, .note {{ margin-left: 0; }}
    .section-meta {{ text-align: left; max-width: none; }}
    .container {{ padding: 0 20px 64px; }}
  }}
  table {{
    width: 100%; border-collapse: collapse;
    font-family: 'JetBrains Mono', 'PingFang SC', 'Microsoft YaHei', monospace;
    font-size: 16px; font-variant-numeric: tabular-nums;
  }}
  thead th {{
    text-align: right; padding: 14px 16px 14px 0;
    border-top: 1px solid var(--ink); border-bottom: 1px solid var(--ink);
    font-family: 'Noto Serif SC', 'PingFang SC', 'Microsoft YaHei', serif;
    font-size: 14px; letter-spacing: 0.04em;
    color: var(--ink); font-weight: 600; white-space: nowrap;
  }}
  thead th:first-child {{ text-align: left; padding-right: 24px; }}
  tbody td {{
    padding: 16px 16px 16px 0; text-align: right;
    border-bottom: 1px dashed var(--hair); vertical-align: baseline;
    white-space: nowrap;
  }}
  tbody td:first-child {{
    text-align: left; font-family: 'Noto Serif SC', 'Spectral', serif;
    font-size: 19px; font-weight: 500; letter-spacing: -0.003em;
    padding-right: 24px;
  }}
  tbody td small {{
    color: var(--muted); font-size: 12px; letter-spacing: 0.04em;
    margin-left: 5px; white-space: nowrap;
  }}
  tbody tr:hover td {{ background: rgba(20,18,17,0.04); }}
  tbody tr:last-child td {{ border-bottom: 1px solid var(--ink); }}
  .up {{ color: var(--up); font-weight: 600; }}
  .down {{ color: var(--down); font-weight: 600; }}
  .strong {{ color: var(--up); font-weight: 700; }}
  .weak {{ color: var(--muted); font-weight: 400; }}
  .tag {{
    display: inline-block; font-family: 'Noto Serif SC', 'PingFang SC', sans-serif;
    font-size: 13px; letter-spacing: 0.04em;
    padding: 4px 11px; border: 1px solid currentColor; font-weight: 500;
  }}
  .tag-up {{ color: var(--up); background: rgba(184,37,31,0.08); }}
  .tag-down {{ color: var(--down); background: rgba(42,95,74,0.08); }}
  .tag-neutral {{ color: var(--muted); background: transparent; }}
  .us-divider {{
    margin: 104px 0 0; padding-top: 72px;
    border-top: 6px double var(--ink); position: relative;
  }}
  .us-divider .stamp {{
    position: absolute; top: -13px; left: 50%; transform: translateX(-50%);
    background: var(--paper); padding: 0 22px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px; letter-spacing: 0.3em; text-transform: uppercase;
    color: var(--ink); white-space: nowrap;
  }}
  .us-divider .stamp em {{ color: var(--up); font-style: normal; }}
  .footer {{
    margin-top: 80px; border-top: 2px solid var(--ink); padding-top: 28px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px; letter-spacing: 0.08em; color: var(--muted);
    line-height: 1.9;
    display: grid; grid-template-columns: 1fr auto; gap: 28px; align-items: end;
  }}
  .footer strong {{
    color: var(--ink); letter-spacing: 0.18em; text-transform: uppercase;
    display: block; margin-bottom: 6px;
  }}
  .footer .colophon {{ text-align: right; font-size: 10px; opacity: 0.75; }}
  @media (max-width: 640px) {{
    .footer {{ grid-template-columns: 1fr; }}
    .footer .colophon {{ text-align: left; }}
    .hero {{ padding: 48px 0 40px; }}
  }}
</style>
</head>
<body>

<div class="tape">
  <div><span class="dot">●</span> QUANT DESK · 主力分析 · LIVE</div>
  <div class="muted">IC-WEIGHTED MULTI-FACTOR · CN-A / US · v3</div>
  <div class="muted">{now}</div>
</div>

<div class="container">

<header class="hero">
  <div class="hero-kicker">Issue № 01 · Research Bulletin · 上海 / 深圳</div>
  <h1>主力<em>分析</em><span class="eyebrow">A-Share Technical &amp; Factor Report</span></h1>
  <div class="hero-meta">
    <span class="pill">Last Sync · {now}</span>
    <button class="btn-refresh" id="refreshBtn" onclick="triggerRefresh()">◉ Refresh Feed</button>
    <span class="refresh-msg" id="refreshMsg"></span>
  </div>
</header>

<script>
async function triggerRefresh() {{
  const btn = document.getElementById('refreshBtn');
  const msg = document.getElementById('refreshMsg');
  btn.disabled = true;
  btn.textContent = 'Syncing...';
  msg.textContent = '';
  let token = localStorage.getItem('gh_token');
  if (!token) {{
    token = prompt('首次使用请输入GitHub Personal Access Token (需要repo和workflow权限):');
    if (!token) {{ btn.disabled = false; btn.textContent = '◉ Refresh Feed'; return; }}
    localStorage.setItem('gh_token', token);
  }}
  try {{
    const r = await fetch('https://api.github.com/repos/maxwu1978/stock-analysis/actions/workflows/update-page.yml/dispatches', {{
      method: 'POST',
      headers: {{ 'Accept': 'application/vnd.github+json', 'Authorization': 'Bearer ' + token }},
      body: JSON.stringify({{ref: 'main'}})
    }});
    if (r.status === 204) {{
      msg.textContent = 'DISPATCHED · 约2分钟后自动刷新';
      msg.style.color = 'var(--down)';
      setTimeout(() => location.reload(), 120000);
    }} else if (r.status === 401 || r.status === 403) {{
      localStorage.removeItem('gh_token');
      msg.textContent = 'TOKEN INVALID · 请重新点击输入';
      msg.style.color = 'var(--up)';
    }} else {{
      msg.textContent = 'FAIL · HTTP ' + r.status;
      msg.style.color = 'var(--up)';
    }}
  }} catch(e) {{
    msg.textContent = 'NET ERROR · ' + e.message;
    msg.style.color = 'var(--up)';
  }}
  btn.disabled = false;
  btn.textContent = '◉ Refresh Feed';
}}
</script>

<section class="section">
  <div class="section-head">
    <div class="section-num">№ 01</div>
    <h2>Quote <em>Board</em><span class="cn">最新行情</span></h2>
    <div class="section-meta">Realtime Tape<br>CN · A-Share</div>
  </div>
  <div class="table-wrap">
  <table>
  <thead><tr><th>股票</th><th>代码</th><th>现价</th><th>涨跌</th><th>成交额</th><th>最高</th><th>最低</th></tr></thead>
  <tbody>
  {quote_html}
  </tbody>
  </table>
  </div>
</section>

<section class="section">
  <div class="section-head">
    <div class="section-num">№ 02</div>
    <h2>Trend <em>Probability</em><span class="cn">趋势概率</span></h2>
    <div class="section-meta">IC-Weighted<br>Rolling Model</div>
  </div>
  <p class="note">Direction flag set by 30-day upside prob · &gt;55 % bias long · &lt;45 % bias short</p>
  <div class="table-wrap">
  <table>
  <thead><tr><th>股票</th><th>方向</th><th>可靠度</th><th>肥尾</th><th>5日</th><th>10日</th><th>30日</th><th>180日</th></tr></thead>
  <tbody>
  {prob_html}
  </tbody>
  </table>
  </div>
</section>

<section class="section">
  <div class="section-head">
    <div class="section-num">№ 03</div>
    <h2>Technical <em>Indicators</em><span class="cn">技术指标</span></h2>
    <div class="section-meta">Oscillators<br>MA / ADX</div>
  </div>
  <div class="table-wrap">
  <table>
  <thead><tr><th>股票</th><th>RSI6</th><th>MACD柱</th><th>MA5</th><th>MA20</th><th>MA60</th><th>ADX</th><th>股性</th></tr></thead>
  <tbody>
  {tech_html}
  </tbody>
  </table>
  </div>
</section>

<section class="section">
  <div class="section-head">
    <div class="section-num">№ 04</div>
    <h2><em>Fundamentals</em><span class="cn">最新财报</span></h2>
    <div class="section-meta">Latest Filing<br>Report Period</div>
  </div>
  <div class="table-wrap">
  <table>
  <thead><tr><th>股票</th><th>报告期</th><th>ROE</th><th>营收增长</th><th>利润增长</th><th>毛利率</th><th>负债率</th></tr></thead>
  <tbody>
  {fund_html}
  </tbody>
  </table>
  </div>
</section>

<div class="footer">
<div>
<strong>Methodology</strong>
Model · 25-factor stack (17 technical + 8 fundamental) × rolling IC weights<br>
Backtest · 5 800 samples / 6 years · multi-regime validation
</div>
<div class="colophon">
Issue № 01 · Vol. IV<br>
Set in DM Serif Display &amp; JetBrains Mono<br>
<strong style="display:inline; font-size:inherit;">Not investment advice</strong>
</div>
</div>

</div>
</body></html>"""

    # ==================== 美股部分 ====================
    print("获取美股行情...")
    try:
        us_rt = fetch_us_realtime()
    except Exception as e:
        print(f"  [!] {e}")
        us_rt = pd.DataFrame()

    print("获取美股财报...")
    try:
        us_fund = fetch_us_financials()
    except Exception as e:
        print(f"  [!] {e}")
        us_fund = {}

    print("获取美股历史数据...")
    try:
        us_hist = fetch_us_all_history("5y")
    except Exception as e:
        print(f"  [!] {e}")
        us_hist = {}
    if allow_partial:
        missing = [name for ticker, name in US_STOCKS.items() if ticker not in us_hist]
        if missing:
            print(f"  [!] 美股历史数据缺失，继续本地预览: {', '.join(missing)}")
    else:
        ensure_complete_dataset(us_hist, "美股", US_STOCKS)

    us_quote_html = ""
    if not us_rt.empty:
        for _, r in us_rt.iterrows():
            chg = r.get("涨跌幅", 0)
            vol = r.get("成交额", 0)
            vol_s = f"{vol/1e6:.1f}M" if vol >= 1e6 else f"{vol:,.0f}"
            mcap = r.get("市值", 0)
            mcap_s = f"${mcap/1e12:.2f}T" if mcap >= 1e12 else f"${mcap/1e9:.0f}B"
            us_quote_html += f'<tr><td>{r["名称"]}</td><td>{r["代码"]}</td>'
            us_quote_html += f'<td>${r.get("最新价",0):.2f}</td>'
            us_quote_html += chg_td(chg)
            us_quote_html += f'<td>{vol_s}</td><td>{mcap_s}</td>'
            us_quote_html += f'<td>${r.get("最高",0):.2f}</td>'
            us_quote_html += f'<td>${r.get("最低",0):.2f}</td></tr>\n'

    us_prob_html = ""
    us_tech_html = ""
    us_fund_html = ""

    for ticker, df in us_hist.items():
        uname = US_STOCKS[ticker]
        ufund = us_fund.get(ticker)
        df = compute_all(df, ufund)
        s = summarize(df)
        if not s:
            continue
        prob = score_trend_us(df)
        if "error" in prob:
            continue
        hp = prob.get("historical_prob", {})
        rg = prob.get("regime", {})

        us_rel = US_SIGNAL_RELIABILITY.get(ticker, "?")
        us_rel_cls = "strong" if us_rel == "强" else ("weak" if us_rel == "弱" else "")

        us_ft = df["fat_tail_score"].iloc[-1] if "fat_tail_score" in df.columns else 0
        if pd.isna(us_ft): us_ft = 0
        us_ft = int(us_ft)
        us_ft_html = f'<td class="strong">{"⚡" * us_ft}</td>' if us_ft >= 3 else '<td>-</td>'

        us_prob_html += f'<tr><td>{uname}</td><td>{direction_tag(hp)}</td><td class="{us_rel_cls}">{us_rel}</td>{us_ft_html}'
        for period in ["5日", "10日", "30日", "180日"]:
            us_prob_html += fmt_prob_cell(hp.get(period))
        us_prob_html += '</tr>\n'

        stype = {"momentum": "动量", "mean_revert": "回归", "mixed": "混合"}.get(rg.get("stock_type"), "?")
        try:
            rsi_v = float(s["RSI6"])
            rsi_cls = "up" if rsi_v > 60 else ("down" if rsi_v < 40 else "")
        except:
            rsi_cls = ""
        us_tech_html += f'<tr><td>{uname}</td>'
        us_tech_html += f'<td class="{rsi_cls}">{s["RSI6"]}</td>'
        us_tech_html += f'<td>{s["MACD"]}</td>'
        us_tech_html += f'<td>{s["MA5"]}</td><td>{s["MA20"]}</td><td>{s["MA60"]}</td>'
        us_tech_html += f'<td>{rg.get("adx",0):.0f}</td><td>{stype}</td></tr>\n'

        if ufund is not None and not ufund.empty:
            # 跳过关键字段全NaN的占位行(未发财报的季度)
            key_cols = [c for c in ["roe", "gross_margin", "debt_ratio", "rev_growth"] if c in ufund.columns]
            mask = ufund[key_cols].notna().any(axis=1) if key_cols else pd.Series([True] * len(ufund))
            ufund_valid = ufund[mask]
            if ufund_valid.empty:
                continue
            lt = ufund_valid.iloc[-1]
            rpt = lt.get("report_date")
            rpt_s = rpt.strftime("%Y-%m-%d") if hasattr(rpt, "strftime") else str(rpt)
            def us_fv(key, fmt=".1f"):
                v = lt.get(key)
                if pd.isna(v): return "-"
                return f"{v:{fmt}}%"
            def us_fv_td(key, fmt=".1f", invert=False):
                v = lt.get(key)
                if pd.isna(v): return '<td>-</td>'
                s_val = f"{v:{fmt}}%"
                if invert:
                    cls = "down" if v > 60 else ("up" if v < 30 else "")
                else:
                    cls = "up" if v > 15 else ("down" if v < 0 else "")
                return f'<td class="{cls}">{s_val}</td>'
            us_fund_html += f'<tr><td>{uname}</td><td>{rpt_s}</td>'
            us_fund_html += us_fv_td("roe")
            us_fund_html += us_fv_td("rev_growth", "+.1f")
            us_fund_html += us_fv_td("profit_growth", "+.1f")
            us_fund_html += f'<td>{us_fv("gross_margin")}</td>'
            us_fund_html += us_fv_td("debt_ratio", ".1f", invert=True)
            us_fund_html += '</tr>\n'

    # 拼接美股HTML
    us_section = f"""
<section class="us-divider">
  <span class="stamp">U.S. Equities · <em>美股研判</em> · NVDA · TSLA · GOOGL · AAPL · FUTU</span>
</section>

<section class="section">
  <div class="section-head">
    <div class="section-num">№ 05</div>
    <h2>Quote <em>Board</em><span class="cn">美股行情</span></h2>
    <div class="section-meta">Realtime<br>NYSE / NASDAQ</div>
  </div>
  <div class="table-wrap">
  <table>
  <thead><tr><th>股票</th><th>代码</th><th>现价</th><th>涨跌</th><th>成交量</th><th>市值</th><th>最高</th><th>最低</th></tr></thead>
  <tbody>{us_quote_html}</tbody>
  </table>
  </div>
</section>

<section class="section">
  <div class="section-head">
    <div class="section-num">№ 06</div>
    <h2>Trend <em>Probability</em><span class="cn">趋势概率</span></h2>
    <div class="section-meta">US-Tuned<br>IC Weights</div>
  </div>
  <div class="table-wrap">
  <table>
  <thead><tr><th>股票</th><th>方向</th><th>可靠度</th><th>肥尾</th><th>5日</th><th>10日</th><th>30日</th><th>180日</th></tr></thead>
  <tbody>{us_prob_html}</tbody>
  </table>
  </div>
</section>

<section class="section">
  <div class="section-head">
    <div class="section-num">№ 07</div>
    <h2>Technical <em>Indicators</em><span class="cn">技术指标</span></h2>
    <div class="section-meta">Oscillators<br>MA / ADX</div>
  </div>
  <div class="table-wrap">
  <table>
  <thead><tr><th>股票</th><th>RSI6</th><th>MACD柱</th><th>MA5</th><th>MA20</th><th>MA60</th><th>ADX</th><th>股性</th></tr></thead>
  <tbody>{us_tech_html}</tbody>
  </table>
  </div>
</section>

<section class="section">
  <div class="section-head">
    <div class="section-num">№ 08</div>
    <h2><em>Fundamentals</em><span class="cn">最新财报</span></h2>
    <div class="section-meta">Latest SEC<br>Filing</div>
  </div>
  <div class="table-wrap">
  <table>
  <thead><tr><th>股票</th><th>报告期</th><th>ROE</th><th>营收增长</th><th>利润增长</th><th>毛利率</th><th>负债率</th></tr></thead>
  <tbody>{us_fund_html}</tbody>
  </table>
  </div>
</section>
"""

    # 在A股footer前插入美股部分
    html = html.replace('<div class="footer">', us_section + '\n<div class="footer">')

    # 期权持仓 section 处理: 本地有最新片段就用新的, 否则从旧 index.html 保留
    # (Actions 环境无 option_section.html, 要避免 Actions 擦除本地 push 的期权内容)
    opt_section_path = "option_section.html"
    opt_html = None
    if os.path.exists(opt_section_path):
        with open(opt_section_path, encoding="utf-8") as f:
            opt_html = f.read()
        print(f"  [+] 期权持仓 section 用本地最新片段 ({len(opt_html)} 字节)")
    else:
        # Actions 环境: 尝试从旧 docs/index.html 中提取期权 section 保留
        old_page_path = "docs/index.html"
        if os.path.exists(old_page_path):
            import re
            with open(old_page_path, encoding="utf-8") as f:
                old_html = f.read()
            # 匹配期权 section 的整块 (包括开头注释到 </section>)
            # 也要保留紧急横幅 (如果有)
            m = re.search(
                r'(<!-- 期权持仓 section[\s\S]*?</section>)',
                old_html,
            )
            if m:
                opt_html = m.group(1)
                # 同时提取紧急横幅 (如果有)
                banner_m = re.search(
                    r'(<div style="background:#cf222e[\s\S]*?</script>)',
                    old_html,
                )
                if banner_m:
                    opt_html = banner_m.group(1) + "\n" + opt_html
                print(f"  [+] 期权持仓 section 从旧页保留 ({len(opt_html)} 字节, 预计 Actions 环境)")

    if opt_html:
        html = html.replace('<div class="footer">', opt_html + '\n<div class="footer">')

    # 真实盘 section **不嵌入公开主页** (隐私保护)
    # 如果存在 real_position_section.html, 说明本地生成了, 但 docs/ 是公开的,
    # 不能暴露真实持仓细节. 真实盘数据只在本地查看 (real_position_local.html).
    real_section_path = "real_position_section.html"
    if os.path.exists(real_section_path):
        print(f"  [!] 检测到 {real_section_path} 但**不会嵌入公开主页**以保护真实持仓隐私")

    # 写到 docs/index.html (GitHub Pages 从 docs 目录读取)
    os.makedirs("docs", exist_ok=True)
    with open("docs/index.html", "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\n页面已生成: docs/index.html ({len(html)} 字节)")
    print("下一步: git add docs && git commit && git push")


if __name__ == "__main__":
    args = parse_args()
    try:
        generate(allow_partial=args.allow_partial)
    except Exception as e:
        print(f"\n[FAIL] 页面生成失败: {e}", file=sys.stderr)
        sys.exit(1)
