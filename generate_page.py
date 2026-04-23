#!/usr/bin/env python3
"""生成静态HTML页面, 用于GitHub Pages部署"""

import argparse
import os
import re
import subprocess
import sys
from datetime import datetime
import pandas as pd

from fetch_data import STOCKS, fetch_realtime_quotes, fetch_all_history
from indicators import compute_all, summarize
from probability import score_trend
from probability_us import score_trend_us
from fundamental import fetch_all_financials
from fetch_us import US_STOCKS, fetch_us_realtime, fetch_us_all_history, fetch_us_financials
from reliability import get_reliability_label, load_reliability_labels
from position_sizing import recommend_model_action
from production_review import load_signal_history, load_trade_log, summarize_execution_quality, summarize_plan_coverage


def ensure_complete_dataset(all_hist: dict, label: str, expected: dict) -> None:
    """关键标的缺失时直接失败，避免发布残缺页面。"""
    missing = [f"{name}({code})" for code, name in expected.items() if code not in all_hist]
    if missing:
        raise RuntimeError(f"{label}历史数据缺失: {', '.join(missing)}")


def ensure_complete_reliability_labels(labels: dict, allow_partial: bool) -> None:
    """发布模式必须使用完整的自动可靠度标签，避免静默回退到 ?。"""
    if not labels:
        msg = "可靠度标签缺失，请先运行 python3 build_reliability.py"
        if allow_partial:
            print(f"  [!] {msg}")
            return
        raise RuntimeError(msg)

    missing_a = [f"{name}({code})" for code, name in STOCKS.items() if code not in labels.get("a_share", {})]
    missing_us = [f"{name}({ticker})" for ticker, name in US_STOCKS.items() if ticker not in labels.get("us", {})]
    if missing_a or missing_us:
        missing = missing_a + missing_us
        msg = f"可靠度标签不完整: {', '.join(missing)}"
        if allow_partial:
            print(f"  [!] {msg}")
            return
        raise RuntimeError(msg)


def extract_old_block(old_html: str, pattern: str) -> str | None:
    match = re.search(pattern, old_html, re.S)
    return match.group(1) if match else None


def refresh_cn_macro_banner(section_html: str, banner_html: str) -> str:
    """Keep the latest CN macro banner even when reusing old A-share sections."""
    trend_pattern = r'(<section class="section" id="cn-trend">[\s\S]*?<div class="section-head">[\s\S]*?</div>)'

    cleaned = re.sub(
        r'\s*<div class="macro-banner">[\s\S]*?</div>\s*(?=<p class="note">)',
        "\n",
        section_html,
        count=1,
    )
    if not banner_html:
        return cleaned

    return re.sub(
        trend_pattern,
        r"\1\n  " + banner_html,
        cleaned,
        count=1,
    )


def extract_section_by_id(html: str, section_id: str) -> str | None:
    pattern = rf'(<section class="section" id="{section_id}">[\s\S]*?</section>)'
    match = re.search(pattern, html, re.S)
    return match.group(1) if match else None


def replace_section_by_id(html: str, section_id: str, replacement: str) -> str:
    pattern = rf'(<section class="section" id="{section_id}">[\s\S]*?</section>)'
    return re.sub(pattern, replacement, html, count=1, flags=re.S)


def load_old_page() -> str:
    old_page_path = "docs/dashboard_full.html"
    if not os.path.exists(old_page_path):
        old_page_path = "docs/index.html"
    if not os.path.exists(old_page_path):
        return ""
    with open(old_page_path, encoding="utf-8") as f:
        return f.read()


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


def prob_item_html(label, d):
    if not d:
        return (
            f'<div class="prob-item prob-empty">'
            f'<span class="period">{label}</span>'
            f'<span class="prob-main">-</span>'
            f'<span class="prob-return">-</span>'
            f'<small>n=0</small>'
            f'</div>'
        )
    p = int(d["上涨概率"].replace("%", ""))
    avg = d["平均收益"]
    n = d["样本数"]
    cls = "prob-item"
    if p >= 60:
        cls += " strong"
    elif p <= 40:
        cls += " weak"
    return (
        f'<div class="{cls}">'
        f'<span class="period">{label}</span>'
        f'<span class="prob-main">{p}%</span>'
        f'<span class="prob-return">{avg}</span>'
        f'<small>n={n}</small>'
        f'</div>'
    )


def fmt_prob_matrix(hp):
    cells = "".join(prob_item_html(period, hp.get(period)) for period in ["5日", "10日", "30日", "180日"])
    return f'<td class="prob-matrix-cell" data-label="概率矩阵"><div class="prob-grid">{cells}</div></td>'


def chg_td(val):
    if not isinstance(val, (int, float)):
        return f'<td>{val}</td>'
    sign = "+" if val >= 0 else ""
    cls = "up" if val >= 0 else "down"
    return f'<td class="{cls}">{sign}{val:.2f}%</td>'


def render_review_section() -> str:
    signals = load_signal_history()
    trades = load_trade_log()
    execution_quality = summarize_execution_quality(trades)
    plan_coverage = summarize_plan_coverage(trades)
    planned_trade_count = 0 if trades.empty or "has_plan" not in trades.columns else int(trades["has_plan"].fillna(False).sum())
    empty_note_html = ""
    if execution_quality.empty and plan_coverage.empty:
        empty_note_html = f"""
  <p class="note">
    当前复盘页没有显示出有效表格，原因通常是<strong>执行样本还不够</strong>，不是页面坏了。
    目前已解析信号 <strong>{len(signals)}</strong> 条，交易日志 <strong>{len(trades)}</strong> 条，
    带计划字段的执行记录 <strong>{planned_trade_count}</strong> 条。
    等你继续按带 `signal_id / plan_tier / plan_exit` 的命令执行后，这里会开始显示执行质量、仓位层级和计划覆盖。
  </p>
"""

    metric_rows = ""
    if execution_quality.empty:
        metric_rows = '<tr><td colspan="2">暂无执行样本</td></tr>'
    else:
        for _, r in execution_quality.iterrows():
            metric_rows += f"<tr><td>{r['metric']}</td><td>{r['value']}</td></tr>\n"

    coverage_rows = ""
    if plan_coverage.empty:
        coverage_rows = '<tr><td colspan="3">暂无带计划字段的执行记录</td></tr>'
    else:
        for _, r in plan_coverage.head(8).iterrows():
            coverage_rows += (
                f"<tr><td>{r.get('plan_tier','-') or '-'}</td>"
                f"<td>{r.get('plan_exit','-') or '-'}</td>"
                f"<td>{r.get('actions',0)}</td></tr>\n"
            )

    return f"""
<section class="section" id="review-block">
  <div class="section-head">
    <div class="section-num">№ 10</div>
    <h2>Execution <em>Review</em><span class="cn">执行评分卡</span></h2>
    <div class="section-meta">Trade Log<br>Plan Coverage</div>
  </div>
  {empty_note_html}
  <div class="table-wrap">
    <table>
      <thead><tr><th>执行质量</th><th>数值</th></tr></thead>
      <tbody>{metric_rows}</tbody>
    </table>
  </div>
  <div class="table-wrap" style="margin-top:18px;">
    <table>
      <thead><tr><th>仓位层级</th><th>退出模板</th><th>动作数</th></tr></thead>
      <tbody>{coverage_rows}</tbody>
    </table>
  </div>
</section>
"""


def extract_style_block(page_html: str) -> str:
    match = re.search(r"<style>([\s\S]*?)</style>", page_html)
    return match.group(1) if match else ""


def extract_footer_block(page_html: str) -> str:
    match = re.search(r'(<div class="footer" id="method-block">[\s\S]*?</div>)\s*</div>\s*</body>', page_html)
    return match.group(1) if match else '<div class="footer" id="method-block"></div>'


def extract_summary_strip_html(page_html: str) -> str:
    match = re.search(r'<section class="summary-strip">([\s\S]*?)</section>', page_html)
    return match.group(1).strip() if match else ""


def render_subpage(
    *,
    title: str,
    hero_kicker: str,
    hero_title_html: str,
    eyebrow: str,
    now: str,
    style_block: str,
    summary_cards_html: str,
    nav_links_html: str,
    body_html: str,
    footer_html: str,
    hero_link_html: str = "",
) -> str:
    summary_section_html = (
        f"""
<section class="summary-strip">
{summary_cards_html}
</section>
"""
        if summary_cards_html.strip()
        else ""
    )
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=Spectral:ital,wght@0,300;0,400;0,500;1,400&family=JetBrains+Mono:wght@400;500;600&family=Noto+Serif+SC:wght@400;500;700&display=swap" rel="stylesheet">
<style>
{style_block}
</style>
</head>
<body>

<div class="tape">
  <div><span class="dot">●</span> QUANT DESK · 主力分析 · LIVE</div>
  <div class="muted">STATIC SUBPAGE · OPTIONS / REVIEW</div>
  <div class="muted">{now}</div>
</div>

<div class="container">

<header class="hero">
  <div class="hero-kicker">{hero_kicker}</div>
  <h1>{hero_title_html}<span class="eyebrow">{eyebrow}</span></h1>
  <div class="hero-meta">
    <span class="pill">Last Sync · {now}</span>
    <span class="pill">Linked from index.html</span>
    {hero_link_html}
  </div>
</header>

{summary_section_html}

<nav class="anchor-nav">
{nav_links_html}
</nav>

{body_html}

{footer_html}

</div>
</body></html>"""


def summarize_execution_strip(signals: list[dict], trades: pd.DataFrame) -> tuple[str, str]:
    executable_actions = {"BUILD_LONG", "PROBE_LONG"}
    watch_actions = {"WATCHLIST", "OBSERVE", "WAIT"}

    exec_count = sum(1 for s in signals if s.get("action") in executable_actions)
    standard_count = sum(1 for s in signals if s.get("tier") == "STANDARD")
    probe_count = sum(1 for s in signals if s.get("tier") == "PROBE")
    micro_count = sum(1 for s in signals if s.get("tier") == "MICRO")
    watch_count = sum(1 for s in signals if s.get("action") in watch_actions)

    win_summary = "胜率 N/A"
    if not trades.empty and "timestamp" in trades.columns:
        pnl_candidates = ["realized_pnl", "pnl", "profit", "net_pnl"]
        pnl_col = next((c for c in pnl_candidates if c in trades.columns), None)
        if pnl_col:
            dated = trades.dropna(subset=["timestamp"]).copy()
            now_ts = pd.Timestamp.now()
            for label, days in [("7D", 7), ("30D", 30)]:
                recent = dated[dated["timestamp"] >= now_ts - pd.Timedelta(days=days)]
                realized = recent[recent[pnl_col].notna()]
                if len(realized) >= 3:
                    win_rate = (realized[pnl_col].astype(float) > 0).mean() * 100
                    win_summary = f"{label} 胜率 {win_rate:.0f}%"
                    break

    value = f"{exec_count} 可执行 / {watch_count} 观察"
    sub = f"STD {standard_count} · PROBE {probe_count} · MICRO {micro_count} · {win_summary}"
    return value, sub


def summarize_strategy_candidates(signals: list[dict]) -> tuple[list[dict], list[dict]]:
    actionable = [s for s in signals if s.get("action") in {"BUILD_LONG", "PROBE_LONG"}]
    watchlist = [s for s in signals if s.get("action") in {"WATCHLIST", "OBSERVE", "WAIT"}]
    actionable = sorted(
        actionable,
        key=lambda x: ({"STANDARD": 0, "PROBE": 1, "MICRO": 2, "NO_TRADE": 3}.get(x.get("tier", ""), 9), -(x.get("score") or 0)),
    )
    watchlist = sorted(watchlist, key=lambda x: (x.get("market", ""), x.get("name", "")))
    return actionable, watchlist


def collect_option_strategy_signals() -> list[dict]:
    signals: list[dict] = []

    if os.path.exists("option_section.html"):
        try:
            option_text = open("option_section.html", encoding="utf-8").read()
            for symbol, action, detail in re.findall(
                r'<td><strong>([A-Z]+)</strong>.*?</td>.*?<td><span class="tag[^"]*">[^<]*\s*([^<]+)</span>\s*<br><small[^>]*>([^<]+)</small>',
                option_text,
                re.S,
            ):
                signals.append({
                    "kind": "holding",
                    "symbol": symbol,
                    "label": f"{symbol} 持仓管理",
                    "action": action.strip(),
                    "strength": "持仓管理",
                    "plan": detail.strip(),
                    "note": "现有仓位以退出模板和时间窗管理为主。",
                })
        except Exception:
            pass

    advisor_specs = [
        ("single", ["python3", "option_fractal_advisor.py", "tech"]),
        ("straddle", ["python3", "option_straddle_advisor.py", "tech"]),
    ]
    for kind, cmd in advisor_specs:
        try:
            run = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="ignore",
                cwd=os.getcwd(),
                timeout=45,
            )
        except Exception:
            continue
        out = run.stdout or ""
        if not out:
            continue

        if kind == "single":
            blocks = re.findall(r"─+\n(.*?)(?=\n─+\n|\n═+|\Z)", out, re.S)
            for block in blocks:
                symbol_match = re.search(r"US\.([A-Z]+)", block)
                advice_match = re.search(r"建议:\s+([A-Z_]+)", block)
                if not symbol_match or not advice_match:
                    continue
                symbol = symbol_match.group(1)
                action = advice_match.group(1)
                if action == "OBSERVE":
                    signals.append({
                        "kind": "single",
                        "symbol": symbol,
                        "label": f"{symbol} 单腿分形",
                        "action": action,
                        "strength": "无机会",
                        "plan": "当前无明确信号",
                        "note": "分形单腿策略当前仅给出观望，不建议开新仓。",
                    })
                else:
                    signals.append({
                        "kind": "single",
                        "symbol": symbol,
                        "label": f"{symbol} 单腿分形",
                        "action": action,
                        "strength": "强机会",
                        "plan": "按顾问输出执行",
                        "note": "出现单腿明确信号，可优先关注。",
                    })
        else:
            blocks = re.findall(r"─+\n(.*?)(?=\n─+\n|\n═+|\Z)", out, re.S)
            for block in blocks:
                symbol_match = re.search(r"US\.([A-Z]+)", block)
                signal_match = re.search(r"信号:\s+([A-Z_]+)\s+置信度:\s+([A-Z]+|None)", block)
                if not symbol_match or not signal_match:
                    continue
                symbol = symbol_match.group(1)
                action = signal_match.group(1)
                confidence = signal_match.group(2)
                if action in {"WAIT", "WAIT_IV_HIGH", "WAIT_IV_HIGH_SELL_CANDIDATE"}:
                    signals.append({
                        "kind": "straddle",
                        "symbol": symbol,
                        "label": f"{symbol} 跨式策略",
                        "action": action,
                        "strength": "无机会",
                        "plan": "当前无跨式机会",
                        "note": "当前波动/IV 结构不支持新开跨式。",
                    })
                    continue
                plan_match = re.search(r"仓位:\s+([^\n]+)", block)
                plan = plan_match.group(1).strip() if plan_match else "按顾问输出执行"
                strength = "弱机会" if "WEAK" in action or confidence == "LOW" else "强机会"
                note = (
                    "跨式弱机会，只适合微型试单。MICRO 表示最小试探仓，套数按风险预算 ÷ 单套风险估算。"
                    if strength == "弱机会"
                    else "跨式出现较清晰波动信号。"
                )
                signals.append({
                    "kind": "straddle",
                    "symbol": symbol,
                    "label": f"{symbol} 跨式策略",
                    "action": action,
                    "strength": strength,
                    "plan": plan,
                    "note": note,
                })

    deduped: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for item in signals:
        key = (item.get("kind", ""), item.get("symbol", ""))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    strength_rank = {"强机会": 0, "弱机会": 1, "持仓管理": 2, "无机会": 3}
    deduped.sort(key=lambda x: (strength_rank.get(x.get("strength", ""), 9), x.get("symbol", "")))
    return deduped


def build_strategy_page(
    full_page_html: str,
    now: str,
    signals: list[dict],
    execution_summary_value: str,
    execution_summary_sub: str,
) -> str:
    style_block = extract_style_block(full_page_html)
    footer_html = extract_footer_block(full_page_html)
    actionable, watchlist = summarize_strategy_candidates(signals)
    option_signals = collect_option_strategy_signals()

    if actionable:
        actionable_rows = ""
        for row in actionable:
            actionable_rows += (
                f"<tr><td>{row.get('market')}</td><td>{row.get('name')}</td><td>{row.get('action')}</td>"
                f"<td>{row.get('tier')}</td><td>{row.get('reliability')}</td>"
                f"<td>{row.get('plan_text')}</td><td>{row.get('rationale')}</td></tr>\n"
            )
        actionable_html = f"""
<div class="table-wrap">
  <table>
    <thead><tr><th>市场</th><th>标的</th><th>动作</th><th>层级</th><th>可靠度</th><th>仓位计划</th><th>原因</th></tr></thead>
    <tbody>{actionable_rows}</tbody>
  </table>
</div>
"""
        actionable_note = "<p class=\"note\">当前有明确信号，优先关注这里；只有这一块才视为接近可执行。</p>"
    else:
        actionable_html = (
            "<p class=\"note\"><strong>当前没有明确信号。</strong> 这不是系统失效，而是当前全部信号仍在观察/等待区。"
            " 弱信号阶段默认不交易，只保留观察名单。</p>"
        )
        actionable_note = ""

    watch_rows = ""
    for row in watchlist[:20]:
        watch_rows += (
            f"<tr><td>{row.get('market')}</td><td>{row.get('name')}</td><td>{row.get('action')}</td>"
            f"<td>{row.get('tier')}</td><td>{row.get('reliability')}</td><td>{row.get('plan_text')}</td></tr>\n"
        )
    watch_html = f"""
<div class="table-wrap">
  <table>
    <thead><tr><th>市场</th><th>标的</th><th>状态</th><th>层级</th><th>可靠度</th><th>计划</th></tr></thead>
    <tbody>{watch_rows or '<tr><td colspan="6">暂无观察名单</td></tr>'}</tbody>
  </table>
</div>
"""

    option_rows = ""
    for row in option_signals:
        option_rows += (
            f"<tr><td>{row.get('label')}</td><td>{row.get('strength')}</td><td>{row.get('action')}</td>"
            f"<td>{row.get('plan')}</td><td>{row.get('note')}</td></tr>\n"
        )
    option_html = f"""
<div class="table-wrap">
  <table>
    <thead><tr><th>策略</th><th>类型</th><th>信号</th><th>计划</th><th>说明</th></tr></thead>
    <tbody>{option_rows or '<tr><td colspan="5">暂无期权机会更新</td></tr>'}</tbody>
  </table>
</div>
"""

    summary_cards_html = f"""
  <article class="summary-card">
    <div class="label">Execution Pulse</div>
    <div class="value">{execution_summary_value}</div>
    <div class="sub">{execution_summary_sub}</div>
  </article>
  <article class="summary-card">
    <div class="label">Actionable Signals</div>
    <div class="value">{len(actionable)}</div>
    <div class="sub">只有这里视为接近可执行</div>
  </article>
  <article class="summary-card">
    <div class="label">Watchlist</div>
    <div class="value">{len(watchlist)}</div>
    <div class="sub">弱信号或等待阶段默认不交易</div>
  </article>
  <article class="summary-card">
    <div class="label">Option Setup</div>
    <div class="value">{sum(1 for row in option_signals if row.get('strength') in {'强机会', '弱机会'})}</div>
    <div class="sub">全池扫描后按强/弱机会与持仓管理分开显示</div>
  </article>
"""
    nav_links_html = """
  <a class="major" href="./index.html">← 返回总览</a>
  <a class="major" href="#strategy-actionable">Actionable</a>
  <a class="major" href="#strategy-watchlist">Watchlist</a>
  <a class="major" href="#strategy-options">Options</a>
  <a class="major" href="./review.html">Review</a>
"""
    body_html = f"""
<section class="section" id="strategy-actionable">
  <div class="section-head">
    <div class="section-num">№ 01</div>
    <h2>Actionable <em>Signals</em><span class="cn">今日明确信号</span></h2>
    <div class="section-meta">Only Trade<br>When Clear</div>
  </div>
  {actionable_note}
  {actionable_html}
</section>

<section class="section" id="strategy-watchlist">
  <div class="section-head">
    <div class="section-num">№ 02</div>
    <h2>Watchlist <em>Only</em><span class="cn">观察名单</span></h2>
    <div class="section-meta">No Trade<br>When Weak</div>
  </div>
  <p class="note">信号弱的时候不交易是正常策略，不需要强行出手。</p>
  {watch_html}
</section>

<section class="section" id="strategy-options">
  <div class="section-head">
    <div class="section-num">№ 03</div>
    <h2>Option <em>Setups</em><span class="cn">今日期权机会</span></h2>
    <div class="section-meta">Strong / Weak<br>Hold Mgmt</div>
  </div>
  <p class="note">这里基于期权顾问关注池做全池扫描，单独区分强机会、弱机会和仅持仓管理。弱信号存在并不等于要交易，默认仍以轻仓或不交易为主。</p>
  {option_html}
</section>
"""
    return render_subpage(
        title="主力分析 · Strategy Desk",
        hero_kicker="Issue № ST · Action Layer · Strategy",
        hero_title_html="交易<em>策略</em>",
        eyebrow="Actionable Signals & Watchlist Only",
        now=now,
        style_block=style_block,
        summary_cards_html=summary_cards_html,
        nav_links_html=nav_links_html,
        body_html=body_html,
        footer_html=footer_html,
        hero_link_html='<a class="pill" href="./index.html" style="text-decoration:none;">← 返回总览</a>',
    )


def build_us_page(full_page_html: str, now: str, us_section: str, macro_headline: str) -> str:
    style_block = extract_style_block(full_page_html)
    footer_html = extract_footer_block(full_page_html)
    summary_cards_html = f"""
  <article class="summary-card">
    <div class="label">Market Focus</div>
    <div class="value">U.S. Desk</div>
    <div class="sub">美股独立详情页</div>
  </article>
  <article class="summary-card">
    <div class="label">Macro Window</div>
    <div class="value">{macro_headline}</div>
    <div class="sub">事件风控优先显示</div>
  </article>
"""
    nav_links_html = """
  <a class="major" href="./index.html">← 返回总览</a>
  <a class="minor" href="#us-quote">US Quote</a>
  <a class="minor" href="#us-trend">US Trend</a>
  <a class="minor" href="#us-tech">US Tech</a>
  <a class="minor" href="#us-fund">US Fund</a>
"""
    return render_subpage(
        title="主力分析 · U.S. Desk",
        hero_kicker="Issue № US · Research Bulletin · New York / Nasdaq",
        hero_title_html="美股<em>子页</em>",
        eyebrow="U.S. Quote, Trend, Tech & Filing Monitor",
        now=now,
        style_block=style_block,
        summary_cards_html=summary_cards_html,
        nav_links_html=nav_links_html,
        body_html=us_section,
        footer_html=footer_html,
        hero_link_html='<a class="pill" href="./index.html" style="text-decoration:none;">← 返回总览</a>',
    )


def build_cn_page(full_page_html: str, now: str, cn_section: str) -> str:
    style_block = extract_style_block(full_page_html)
    footer_html = extract_footer_block(full_page_html)
    if not cn_section:
        match = re.search(
            r'(<div class="market-block cn-block" id="cn-block">[\s\S]*?)\s*(?=<div class="market-block us-block"|<div class="position-panel" id="option-block"|<section class="section" id="review-block"|<div class="footer" id="method-block")',
            full_page_html,
        )
        cn_section = match.group(1) if match else ""
    summary_cards_html = ""
    nav_links_html = """
  <a class="major" href="./index.html">← 返回总览</a>
  <a class="major" href="#cn-block">A-Share</a>
  <a class="minor" href="#cn-quote">CN Quote</a>
  <a class="minor" href="#cn-trend">CN Trend</a>
  <a class="minor" href="#cn-tech">CN Tech</a>
  <a class="minor" href="#cn-fund">CN Fund</a>
"""
    return render_subpage(
        title="主力分析 · A-Share Desk",
        hero_kicker="Issue № CN · Research Bulletin · 上海 / 深圳",
        hero_title_html="A股<em>子页</em>",
        eyebrow="A-Share Quote, Trend, Tech & Filing Monitor",
        now=now,
        style_block=style_block,
        summary_cards_html=summary_cards_html,
        nav_links_html=nav_links_html,
        body_html=cn_section,
        footer_html=footer_html,
        hero_link_html='<a class="pill" href="./index.html" style="text-decoration:none;">← 返回总览</a>',
    )


def build_overview_page(
    *,
    full_page_html: str,
    now: str,
    summary_cards_html: str,
    a_weak: int,
    a_total: int,
    us_mid_summary: str,
    option_summary: str,
    execution_summary_value: str,
    execution_summary_sub: str,
) -> str:
    style_block = extract_style_block(full_page_html)
    footer_html = extract_footer_block(full_page_html)
    nav_links_html = """
  <a class="major" href="./cn.html">A-Share</a>
  <a class="major" href="./us.html">U.S.</a>
  <a class="major" href="./strategy.html">Strategy</a>
  <a class="major" href="./options.html">Options</a>
  <a class="major" href="./review.html">Review</a>
  <a class="major" href="#overview-hub">Overview</a>
"""
    body_html = f"""
<section class="section" id="overview-hub">
  <div class="section-head">
    <div class="section-num">№ 01</div>
    <h2>Overview <em>Hub</em><span class="cn">总览导航</span></h2>
    <div class="section-meta">Multi-page<br>Fast Browse</div>
  </div>
  <div class="table-wrap">
    <table>
      <thead><tr><th>模块</th><th>当前摘要</th><th>入口</th></tr></thead>
      <tbody>
        <tr><td>A股</td><td>{a_weak}/{a_total} 弱，主模型偏研究参考</td><td><a href="./cn.html">打开 A股子页</a></td></tr>
        <tr><td>美股</td><td>{us_mid_summary} 相对居前，宏观覆盖已接入</td><td><a href="./us.html">打开 美股子页</a></td></tr>
        <tr><td>策略</td><td>只把明确信号单独列出，弱信号默认不交易</td><td><a href="./strategy.html">打开 策略子页</a></td></tr>
        <tr><td>期权</td><td>{option_summary}，退出模板状态单独查看</td><td><a href="./options.html">打开 期权子页</a></td></tr>
        <tr><td>复盘</td><td>{execution_summary_value}，{execution_summary_sub}</td><td><a href="./review.html">打开 复盘子页</a></td></tr>
      </tbody>
    </table>
  </div>
</section>
"""
    return render_subpage(
        title="主力分析 · Overview",
        hero_kicker="Issue № 01 · Control Deck · Overview",
        hero_title_html="主力<em>总览</em>",
        eyebrow="Overview, Navigation & Execution Pulse",
        now=now,
        style_block=style_block,
        summary_cards_html=summary_cards_html,
        nav_links_html=nav_links_html,
        body_html=body_html,
        footer_html=footer_html,
    )


def build_options_page(
    full_page_html: str,
    now: str,
    option_html: str | None,
    option_summary: str,
    execution_summary_value: str,
    execution_summary_sub: str,
    macro_headline: str,
) -> str:
    style_block = extract_style_block(full_page_html)
    footer_match = re.search(r'(<div class="footer" id="method-block">[\s\S]*?</div>)', full_page_html)
    footer_html = footer_match.group(1) if footer_match else ""
    if option_html:
        body_html = option_html if 'id="option-block"' in option_html else f'<div class="position-panel" id="option-block">{option_html}</div>'
    else:
        body_html = """
<div class="position-panel" id="option-block">
  <section class="section">
    <div class="section-head">
      <div class="section-num">№ 09</div>
      <h2><em>Option</em> Positions<span class="cn">期权持仓</span></h2>
      <div class="section-meta">SIMULATE<br>暂无片段</div>
    </div>
    <p class="note">当前未找到 option_section.html，期权子页保留占位。</p>
  </section>
</div>
"""
    summary_cards_html = f"""
  <article class="summary-card">
    <div class="label">Option PnL</div>
    <div class="value">{option_summary}</div>
    <div class="sub">来自最新期权监控片段</div>
  </article>
  <article class="summary-card">
    <div class="label">Execution Pulse</div>
    <div class="value">{execution_summary_value}</div>
    <div class="sub">{execution_summary_sub}</div>
  </article>
  <article class="summary-card">
    <div class="label">Macro Window</div>
    <div class="value">{macro_headline}</div>
    <div class="sub">用于解释波动兑现与收缩</div>
  </article>
  <article class="summary-card">
    <div class="label">Navigation</div>
    <div class="value">Home / Review</div>
    <div class="sub">回主面板或执行评分卡</div>
  </article>
  <article class="summary-card">
    <div class="label">Scope</div>
    <div class="value">№ 09</div>
    <div class="sub">仅期权区，不混入研究主表</div>
  </article>
"""
    nav_links_html = """
  <a class="major" href="./index.html">← 返回总览</a>
  <a class="major" href="#option-block">Options</a>
  <a class="major" href="./review.html">Review</a>
  <a class="major" href="./index.html#method-block">Method</a>
"""
    return render_subpage(
        title="期权持仓 · QUANT DESK",
        hero_kicker="Issue № 09 · Options Sidecar · Monitor Panel",
        hero_title_html="期权<em>持仓</em>",
        eyebrow="Option Positions & Exit Tracking",
        now=now,
        style_block=style_block,
        summary_cards_html=summary_cards_html,
        nav_links_html=nav_links_html,
        body_html=body_html,
        footer_html=footer_html,
        hero_link_html='<a class="pill" href="./index.html" style="text-decoration:none;">← 返回总览</a>',
    )


def build_review_page(
    full_page_html: str,
    now: str,
    review_html: str,
    option_summary: str,
    execution_summary_value: str,
    execution_summary_sub: str,
    macro_headline: str,
    a_weak: int,
    a_total: int,
) -> str:
    style_block = extract_style_block(full_page_html)
    footer_match = re.search(r'(<div class="footer" id="method-block">[\s\S]*?</div>)', full_page_html)
    footer_html = footer_match.group(1) if footer_match else ""
    summary_cards_html = f"""
  <article class="summary-card">
    <div class="label">Execution Pulse</div>
    <div class="value">{execution_summary_value}</div>
    <div class="sub">{execution_summary_sub}</div>
  </article>
  <article class="summary-card">
    <div class="label">Option PnL</div>
    <div class="value">{option_summary}</div>
    <div class="sub">用于对照执行动作后结果</div>
  </article>
  <article class="summary-card">
    <div class="label">A-Share Reliability</div>
    <div class="value">{a_weak}/{a_total} 弱</div>
    <div class="sub">主模型偏弱，复盘更看执行一致性</div>
  </article>
  <article class="summary-card">
    <div class="label">Macro Window</div>
    <div class="value">{macro_headline}</div>
    <div class="sub">帮助解释收缩、等待与错失</div>
  </article>
  <article class="summary-card">
    <div class="label">Scope</div>
    <div class="value">Scorecard</div>
    <div class="sub">仅执行评分卡与必要摘要导航</div>
  </article>
"""
    nav_links_html = """
  <a class="major" href="./index.html">← 返回总览</a>
  <a class="major" href="./options.html">Options</a>
  <a class="major" href="#review-block">Review</a>
  <a class="major" href="./index.html#method-block">Method</a>
"""
    return render_subpage(
        title="执行复盘 · QUANT DESK",
        hero_kicker="Issue № 10 · Execution Review · Scorecard",
        hero_title_html="执行<em>复盘</em>",
        eyebrow="Production Review & Scorecard",
        now=now,
        style_block=style_block,
        summary_cards_html=summary_cards_html,
        nav_links_html=nav_links_html,
        body_html=review_html,
        footer_html=footer_html,
        hero_link_html='<a class="pill" href="./index.html" style="text-decoration:none;">← 返回总览</a>',
    )


def generate(allow_partial: bool = False):
    footer_marker = '<div class="footer" id="method-block">'
    reliability_labels = load_reliability_labels()
    ensure_complete_reliability_labels(reliability_labels, allow_partial)
    old_page_html = load_old_page()
    trades = load_trade_log()

    a_labels = reliability_labels.get("a_share", {})
    us_labels = reliability_labels.get("us", {})
    a_weak = sum(1 for v in a_labels.values() if v.get("label") == "弱")
    a_total = len(STOCKS)
    us_mid_names = [US_STOCKS[k] for k, v in us_labels.items() if v.get("label") == "中"]
    us_mid_summary = " / ".join(us_mid_names) if us_mid_names else "暂无"

    try:
        from macro_events import get_risk_warnings
        macro_headline = " / ".join(w.split("—")[0].replace("🟡 ", "").replace("⚪ ", "").replace("🔴 ", "").strip()
                                    for w in get_risk_warnings(days_ahead=14)[:2]) or "无显著事件"
    except Exception:
        macro_headline = "事件窗口未知"

    option_summary = "N/A"
    if os.path.exists("option_section.html"):
        try:
            option_text = open("option_section.html", encoding="utf-8").read()
            m = re.search(r'浮动盈亏.*?(\$[+-]?\d+(?:\.\d+)?)', option_text, re.S)
            if not m:
                m = re.search(r'(\$[+-]?\d+(?:\.\d+)?)', option_text)
            if m:
                option_summary = m.group(1)
        except Exception:
            pass

    execution_summary_value = "__EXECUTION_SUMMARY_VALUE__"
    execution_summary_sub = "__EXECUTION_SUMMARY_SUB__"
    signal_snapshots: list[dict] = []

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
    cn_macro_note_html = ""

    try:
        from macro_events import get_cn_risk_warnings
        cn_macro_notes = get_cn_risk_warnings(days_ahead=14)
    except Exception:
        cn_macro_notes = []

    if cn_macro_notes:
        cn_summary = " · ".join(dict.fromkeys(cn_macro_notes[:4]))
        cn_macro_note_html = (
            f'<div class="macro-banner">'
            f'<div class="banner-kicker">CN Macro Window</div>'
            f'<div class="banner-body">{cn_summary}</div>'
            f'<div class="banner-meta">{len(cn_macro_notes)} windows tracked</div>'
            f'</div>'
        )

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

        reliability = get_reliability_label(reliability_labels, "a_share", code)
        rel_cls = "strong" if reliability == "强" else ("weak" if reliability == "弱" else "")
        decision = recommend_model_action(
            direction={
                "5日": hp.get("5日"),
                "10日": hp.get("10日"),
                "30日": hp.get("30日"),
                "180日": hp.get("180日"),
            } and ("看涨" if (hp.get("30日") and int(hp["30日"]["上涨概率"].replace("%", "")) >= 60) else
                     "偏涨" if (hp.get("30日") and int(hp["30日"]["上涨概率"].replace("%", "")) >= 55) else
                     "看跌" if (hp.get("30日") and int(hp["30日"]["上涨概率"].replace("%", "")) <= 35) else
                     "偏跌" if (hp.get("30日") and int(hp["30日"]["上涨概率"].replace("%", "")) <= 45) else "震荡"),
            entry_price=float(df["close"].iloc[-1]),
            score=prob.get("score"),
            reliability=reliability,
            macro_penalty=0,
        )
        signal_snapshots.append({
            "market": "CN",
            "symbol": code,
            "name": name,
            "action": decision.action,
            "tier": decision.plan.position_tier,
            "allowed": decision.plan.allowed,
            "reliability": reliability,
            "score": float(prob.get("score", 0) or 0),
            "plan_text": f"{decision.plan.position_tier} · {decision.plan.qty}股 · ${decision.plan.risk_budget:,.0f}",
            "rationale": decision.rationale,
        })

        ft_score = df["fat_tail_score"].iloc[-1] if "fat_tail_score" in df.columns else 0
        if pd.isna(ft_score):
            ft_score = 0
        ft_score = int(ft_score)
        ft_text = "⚡" * ft_score if ft_score >= 1 else "-"
        ft_cls = " strong" if ft_score >= 3 else ""
        risk_html = (
            f'<td data-label="风险提示">'
            f'<div class="risk-stack">'
            f'<span class="risk-chip{ft_cls}"><strong>{ft_text}</strong></span>'
            f'<span class="risk-meta">Fat Tail</span>'
            f'<span class="risk-chip"><strong>{decision.action}</strong></span>'
            f'<span class="risk-meta">{decision.plan.position_tier} · {decision.plan.qty}股 · ${decision.plan.risk_budget:,.0f}</span>'
            f'</div>'
            f'</td>'
        )

        prob_html += (
            f'<tr>'
            f'<td data-label="股票">{name}</td>'
            f'<td data-label="方向">{direction_tag(hp)}</td>'
            f'<td class="{rel_cls}" data-label="可靠度">{reliability}</td>'
            f'{risk_html}'
        )
        prob_html += fmt_prob_matrix(hp)
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
  .hero {{ padding: 48px 0 34px; border-bottom: 2px solid var(--ink); }}
  .hero-kicker {{
    display: flex; gap: 16px; align-items: center; margin-bottom: 18px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px; letter-spacing: 0.24em; text-transform: uppercase;
    color: var(--muted);
  }}
  .hero-kicker::before {{ content: ''; width: 48px; height: 1px; background: var(--ink); display: inline-block; }}
  .hero h1 {{
    font-family: 'DM Serif Display', 'Noto Serif SC', serif;
    font-weight: 400;
    font-size: clamp(48px, 8vw, 118px);
    line-height: 0.88;
    letter-spacing: -0.028em;
  }}
  .hero h1 em {{ font-style: italic; color: var(--up); }}
  .hero h1 .eyebrow {{
    display: block; font-size: 0.14em; letter-spacing: 0.26em;
    text-transform: uppercase; color: var(--muted);
    font-family: 'JetBrains Mono', monospace; font-style: normal;
    margin-top: 18px; font-weight: 400;
  }}
  .hero-meta {{
    margin-top: 26px;
    display: flex; flex-wrap: wrap; align-items: center; gap: 18px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px; letter-spacing: 0.1em; color: var(--muted);
  }}
  .summary-strip {{
    display: grid;
    grid-template-columns: repeat(5, minmax(0, 1fr));
    gap: 14px;
    margin: 18px 0 26px;
  }}
  .summary-card {{
    background: linear-gradient(180deg, rgba(232,223,205,0.9), rgba(242,237,226,0.92));
    border-top: 2px solid var(--ink);
    border-left: 1px solid var(--hair);
    padding: 14px 16px 16px;
    min-height: 92px;
  }}
  .summary-card .label {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    color: var(--muted);
    margin-bottom: 8px;
  }}
  .summary-card .value {{
    font-family: 'DM Serif Display', 'Noto Serif SC', serif;
    font-size: clamp(24px, 3vw, 34px);
    line-height: 1;
  }}
  .summary-card .sub {{
    margin-top: 8px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    letter-spacing: 0.08em;
    color: var(--muted);
    text-transform: uppercase;
  }}
  .anchor-nav {{
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
    margin: 0 0 18px;
    position: sticky;
    top: 48px;
    z-index: 9;
    padding: 10px 0 12px;
    background: linear-gradient(180deg, rgba(242,237,226,0.96), rgba(242,237,226,0.82));
    backdrop-filter: blur(4px);
  }}
  .anchor-nav a {{
    text-decoration: none;
    color: var(--ink);
    border: 1px solid var(--hair);
    padding: 8px 12px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 10.5px;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    background: rgba(255,255,255,0.22);
  }}
  .anchor-nav a:hover {{ border-color: var(--ink); transform: translateY(-1px); }}
  .anchor-nav a.major {{
    background: rgba(20,18,17,0.06);
    border-color: var(--ink);
  }}
  .anchor-nav a.minor {{
    color: var(--muted);
    border-style: dashed;
  }}
  .market-block {{
    margin: 24px 0 0;
    padding: 24px 20px 14px;
    border: 1px solid var(--hair);
    background: linear-gradient(180deg, rgba(255,255,255,0.10), rgba(232,223,205,0.35));
    position: relative;
  }}
  .market-block + .market-block {{ margin-top: 42px; }}
  .market-label {{
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    gap: 12px;
    padding-bottom: 14px;
    border-bottom: 1px solid var(--hair);
    margin-bottom: 12px;
  }}
  .market-label strong {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px;
    letter-spacing: 0.22em;
    text-transform: uppercase;
  }}
  .market-label span {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: var(--muted);
  }}
  .macro-banner {{
    margin: 4px 0 20px 128px;
    padding: 12px 14px;
    border-left: 6px solid var(--up);
    background: rgba(184,37,31,0.06);
    display: grid;
    grid-template-columns: 160px 1fr auto;
    gap: 14px;
    align-items: start;
  }}
  .macro-banner .banner-kicker,
  .macro-banner .banner-meta {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    letter-spacing: 0.16em;
    text-transform: uppercase;
    color: var(--muted);
  }}
  .macro-banner .banner-body {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px;
    line-height: 1.8;
  }}
  .position-panel {{
    margin-top: 30px;
    padding: 18px 18px 8px;
    border: 2px solid var(--ink);
    background: linear-gradient(180deg, rgba(20,18,17,0.03), rgba(232,223,205,0.50));
  }}
  .position-panel .section:first-child {{
    padding-top: 28px;
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
  .signal-table {{
    table-layout: fixed;
    border-collapse: separate;
    border-spacing: 0;
  }}
  .signal-table thead th,
  .signal-table tbody td {{
    text-align: center;
    vertical-align: top;
    white-space: normal;
  }}
  .signal-table thead th:first-child,
  .signal-table tbody td:first-child {{
    text-align: left;
  }}
  .signal-table thead th:nth-child(1) {{ width: 18%; }}
  .signal-table thead th:nth-child(2) {{ width: 12%; }}
  .signal-table thead th:nth-child(3) {{ width: 10%; }}
  .signal-table thead th:nth-child(4) {{ width: 18%; }}
  .signal-table thead th:nth-child(5) {{ width: 42%; }}
  .signal-table tbody td:nth-child(2),
  .signal-table tbody td:nth-child(3),
  .signal-table tbody td:nth-child(4) {{
    vertical-align: middle;
    font-size: 14px;
  }}
  .signal-table tbody td:nth-child(-n+4) {{
    background: linear-gradient(180deg, rgba(242,237,226,0.98), rgba(232,223,205,0.92));
  }}
  .signal-table thead th:nth-child(-n+4) {{
    background: linear-gradient(180deg, rgba(242,237,226,1), rgba(232,223,205,0.96));
  }}
  .signal-table .prob-matrix-cell {{
    padding-right: 0;
  }}
  .signal-table .prob-grid {{
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 8px;
  }}
  .signal-table .prob-item {{
    border: 1px solid var(--hair);
    background: rgba(255,255,255,0.28);
    padding: 10px 8px 8px;
    line-height: 1.2;
  }}
  .signal-table .prob-item .period {{
    display: block;
    margin-bottom: 6px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: var(--muted);
  }}
  .signal-table .prob-main {{
    display: block;
    font-size: 17px;
    font-weight: 600;
  }}
  .signal-table .prob-return {{
    display: block;
    margin-top: 4px;
    font-size: 13px;
    color: var(--ink);
  }}
  .signal-table .prob-item small {{
    display: block;
    margin-top: 5px;
    font-size: 11px;
    color: var(--muted);
  }}
  .signal-table .prob-empty {{
    color: var(--muted);
  }}
  .signal-table .risk-stack {{
    display: grid;
    gap: 8px;
    justify-items: center;
  }}
  .signal-table .risk-chip {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-width: 78px;
    padding: 5px 8px;
    border: 1px solid var(--hair);
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px;
    letter-spacing: 0.08em;
    background: rgba(255,255,255,0.22);
  }}
  .signal-table .risk-chip strong {{
    font-size: 13px;
  }}
  .signal-table .risk-meta {{
    display: block;
    font-size: 11px;
    line-height: 1.35;
    color: var(--muted);
  }}
  .signal-table .risk-meta strong {{
    color: var(--ink);
  }}
  .us-divider {{
    margin: 36px 0 0; padding-top: 20px;
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
    .hero {{ padding: 36px 0 26px; }}
    .summary-strip {{ grid-template-columns: 1fr 1fr; }}
    .macro-banner {{ grid-template-columns: 1fr; margin-left: 0; }}
    .market-block {{ padding: 18px 12px 6px; }}
  }}
  @media (max-width: 820px) {{
    .summary-strip {{ grid-template-columns: 1fr 1fr; }}
    .anchor-nav {{ margin-bottom: 8px; }}
    .signal-table {{
      min-width: 0;
      width: 100%;
      border-collapse: separate;
    }}
    .signal-table thead {{
      display: none;
    }}
    .signal-table,
    .signal-table tbody,
    .signal-table tr,
    .signal-table td {{
      display: block;
      width: 100%;
    }}
    .signal-table tr {{
      margin: 0 0 16px;
      padding: 14px 14px 8px;
      border: 1px solid var(--hair);
      background: linear-gradient(180deg, rgba(242,237,226,0.98), rgba(232,223,205,0.92));
    }}
    .signal-table tbody td {{
      border-bottom: 1px dashed var(--hair);
      padding: 9px 0;
      text-align: left;
      white-space: normal;
    }}
    .signal-table tbody td::before {{
      content: attr(data-label);
      display: block;
      margin-bottom: 4px;
      font-family: 'JetBrains Mono', monospace;
      font-size: 10px;
      letter-spacing: 0.14em;
      text-transform: uppercase;
      color: var(--muted);
    }}
    .signal-table tbody td:first-child {{
      padding-top: 0;
      padding-bottom: 12px;
      border-bottom: 1px solid var(--ink);
      font-size: 22px;
    }}
    .signal-table tbody td:first-child::before {{
      display: none;
    }}
    .signal-table tbody td:last-child {{
      border-bottom: 0;
      padding-bottom: 2px;
    }}
    .signal-table .prob-grid {{
      grid-template-columns: 1fr 1fr;
    }}
    .signal-table .prob-main {{
      font-size: 18px;
    }}
    .signal-table .risk-stack {{
      justify-items: start;
    }}
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

<section class="summary-strip">
  <article class="summary-card">
    <div class="label">A-Share Reliability</div>
    <div class="value">{a_weak}/{a_total} 弱</div>
    <div class="sub">当前主模型整体偏弱</div>
  </article>
  <article class="summary-card">
    <div class="label">US Relative Leader</div>
    <div class="value">{us_mid_summary}</div>
    <div class="sub">当前自动标签相对最强</div>
  </article>
  <article class="summary-card">
    <div class="label">Macro Window</div>
    <div class="value">{macro_headline}</div>
    <div class="sub">事件风控已接入美股预测</div>
  </article>
  <article class="summary-card">
    <div class="label">Option Panel</div>
    <div class="value">{option_summary}</div>
    <div class="sub">持仓盈亏快照</div>
  </article>
  <article class="summary-card">
    <div class="label">Execution Pulse</div>
    <div class="value">{execution_summary_value}</div>
    <div class="sub">{execution_summary_sub}</div>
  </article>
</section>

<nav class="anchor-nav">
  <a class="major" href="#cn-block">A-Share</a>
  <a class="minor" href="#cn-quote">CN Quote</a>
  <a class="minor" href="#cn-trend">CN Trend</a>
  <a class="major" href="#us-block">U.S.</a>
  <a class="minor" href="#us-quote">US Quote</a>
  <a class="minor" href="#us-trend">US Trend</a>
  <a class="major" href="#option-block">Options</a>
  <a class="major" href="#review-block">Review</a>
  <a class="major" href="#method-block">Method</a>
</nav>

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

<div class="market-block cn-block" id="cn-block">
<div class="market-label"><strong>China Block</strong><span>A-Share Core Board · Quote / Trend / Tech / Filing</span></div>
<section class="section" id="cn-quote">
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

<section class="section" id="cn-trend">
  <div class="section-head">
    <div class="section-num">№ 02</div>
    <h2>Trend <em>Probability</em><span class="cn">趋势概率</span></h2>
    <div class="section-meta">IC-Weighted<br>Rolling Model</div>
  </div>
  {cn_macro_note_html}
  <p class="note">Direction flag set by 30-day upside prob · &gt;55 % bias long · &lt;45 % bias short</p>
  <div class="table-wrap">
  <table class="signal-table">
  <thead><tr><th>股票</th><th>方向</th><th>可靠度</th><th>风险提示</th><th>概率矩阵</th></tr></thead>
  <tbody>
  {prob_html}
  </tbody>
  </table>
  </div>
</section>

<section class="section" id="cn-tech">
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

<section class="section" id="cn-fund">
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
</div>

<div class="footer" id="method-block">
<div>
<strong>Methodology</strong>
Model · slimmed multi-factor stack × rolling IC weights × tear-sheet family priors<br>
Backtest · 5 800 samples / 6 years · multi-regime validation<br>
Reliability · auto-labeled from structured backtests; current A-share / US model signals are mostly weak
</div>
<div class="colophon">
Issue № 01 · Vol. IV<br>
Set in DM Serif Display &amp; JetBrains Mono<br>
<strong style="display:inline; font-size:inherit;">Not investment advice</strong>
</div>
</div>

</div>
</body></html>"""

    if allow_partial and old_page_html:
        if not prob_html.strip():
            old_trend = extract_section_by_id(old_page_html, "cn-trend")
            if old_trend:
                old_trend = refresh_cn_macro_banner(old_trend, cn_macro_note_html)
                html = replace_section_by_id(html, "cn-trend", old_trend)
                print("  [!] A股趋势区块为空，保留旧页面内容并刷新宏观条")
        if not tech_html.strip():
            old_tech = extract_section_by_id(old_page_html, "cn-tech")
            if old_tech:
                html = replace_section_by_id(html, "cn-tech", old_tech)
                print("  [!] A股技术区块为空，保留旧页面内容")
        if not fund_html.strip():
            old_fund = extract_section_by_id(old_page_html, "cn-fund")
            if old_fund:
                html = replace_section_by_id(html, "cn-fund", old_fund)
                print("  [!] A股财报区块为空，保留旧页面内容")
    cn_section = extract_old_block(
        html,
        r'(<div class="market-block cn-block" id="cn-block">[\s\S]*?</div>)\s*<div class="footer" id="method-block">',
    ) or ""

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
    us_macro_notes = []

    for ticker, df in us_hist.items():
        uname = US_STOCKS[ticker]
        ufund = us_fund.get(ticker)
        df = compute_all(df, ufund)
        s = summarize(df)
        if not s:
            continue
        prob = score_trend_us(df, symbol=ticker)
        if "error" in prob:
            continue
        hp = prob.get("historical_prob", {})
        rg = prob.get("regime", {})

        us_rel = get_reliability_label(reliability_labels, "us", ticker)
        us_rel_cls = "strong" if us_rel == "强" else ("weak" if us_rel == "弱" else "")
        macro = prob.get("macro_overlay", {})
        penalty = int(macro.get("penalty", 0) or 0)
        direction = (
            "看涨" if (hp.get("30日") and int(hp["30日"]["上涨概率"].replace("%", "")) >= 60) else
            "偏涨" if (hp.get("30日") and int(hp["30日"]["上涨概率"].replace("%", "")) >= 55) else
            "看跌" if (hp.get("30日") and int(hp["30日"]["上涨概率"].replace("%", "")) <= 35) else
            "偏跌" if (hp.get("30日") and int(hp["30日"]["上涨概率"].replace("%", "")) <= 45) else "震荡"
        )
        decision = recommend_model_action(
            direction=direction,
            entry_price=float(df["close"].iloc[-1]),
            score=prob.get("score"),
            reliability=us_rel,
            macro_penalty=penalty,
        )
        signal_snapshots.append({
            "market": "US",
            "symbol": ticker,
            "name": uname,
            "action": decision.action,
            "tier": decision.plan.position_tier,
            "allowed": decision.plan.allowed,
            "reliability": us_rel,
            "score": float(prob.get("score", 0) or 0),
            "plan_text": f"{decision.plan.position_tier} · {decision.plan.qty}股 · ${decision.plan.risk_budget:,.0f}",
            "rationale": decision.rationale,
        })
        reasons = macro.get("reasons", [])
        if penalty > 0:
            us_macro_notes.extend(macro.get("warnings", []))
            reason_text = "/".join(dict.fromkeys(reasons)) if reasons else "宏观收缩"
            macro_line = f'<span class="risk-chip weak"><strong>-{penalty}</strong></span><span class="risk-meta">{reason_text}</span>'
        else:
            macro_line = '<span class="risk-chip"><strong>-</strong></span><span class="risk-meta">Macro</span>'

        us_ft = df["fat_tail_score"].iloc[-1] if "fat_tail_score" in df.columns else 0
        if pd.isna(us_ft):
            us_ft = 0
        us_ft = int(us_ft)
        us_ft_text = "⚡" * us_ft if us_ft >= 1 else "-"
        us_ft_cls = " strong" if us_ft >= 3 else ""
        us_risk_html = (
            f'<td data-label="风险提示">'
            f'<div class="risk-stack">'
            f'{macro_line}'
            f'<span class="risk-chip{us_ft_cls}"><strong>{us_ft_text}</strong></span>'
            f'<span class="risk-meta">Fat Tail</span>'
            f'<span class="risk-chip"><strong>{decision.action}</strong></span>'
            f'<span class="risk-meta">{decision.plan.position_tier} · {decision.plan.qty}股 · ${decision.plan.risk_budget:,.0f}</span>'
            f'</div>'
            f'</td>'
        )

        us_prob_html += (
            f'<tr>'
            f'<td data-label="股票">{uname}</td>'
            f'<td data-label="方向">{direction_tag(hp)}</td>'
            f'<td class="{us_rel_cls}" data-label="可靠度">{us_rel}</td>'
            f'{us_risk_html}'
        )
        us_prob_html += fmt_prob_matrix(hp)
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
    us_macro_note_html = ""
    if us_macro_notes:
        summary = " · ".join(dict.fromkeys(us_macro_notes))
        affected = sum(1 for row in us_prob_html.split("</tr>") if "risk-chip weak" in row)
        max_penalty = max([int(x) for x in re.findall(r"<strong>-(\d+)</strong>", us_prob_html)] or [0])
        us_macro_note_html = (
            f'<div class="macro-banner">'
            f'<div class="banner-kicker">Macro Overlay</div>'
            f'<div class="banner-body">{summary}</div>'
            f'<div class="banner-meta">{affected} names affected · max -{max_penalty}</div>'
            f'</div>'
        )

    us_section = f"""
<div class="market-block us-block" id="us-block">
<div class="market-label"><strong>U.S. Block</strong><span>Macro-aware Model · Quote / Trend / Tech / Filing</span></div>
<section class="us-divider">
  <span class="stamp">U.S. Equities · <em>美股研判</em> · NVDA · TSM · MU · WDC · GOOGL · AAPL</span>
</section>

<section class="section" id="us-quote">
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

<section class="section" id="us-trend">
  <div class="section-head">
    <div class="section-num">№ 06</div>
    <h2>Trend <em>Probability</em><span class="cn">趋势概率</span></h2>
    <div class="section-meta">US-Tuned<br>IC Weights</div>
  </div>
  {us_macro_note_html}
  <div class="table-wrap">
  <table class="signal-table">
  <thead><tr><th>股票</th><th>方向</th><th>可靠度</th><th>风险提示</th><th>概率矩阵</th></tr></thead>
  <tbody>{us_prob_html}</tbody>
  </table>
  </div>
</section>

<section class="section" id="us-tech">
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

<section class="section" id="us-fund">
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

</div>
"""

    if allow_partial and not any([us_quote_html.strip(), us_prob_html.strip(), us_tech_html.strip(), us_fund_html.strip()]) and old_page_html:
        old_us = extract_old_block(
            old_page_html,
            r'(<section class="us-divider">[\s\S]*?</section>\s*<section class="section">[\s\S]*?</section>\s*<section class="section">[\s\S]*?</section>\s*<section class="section">[\s\S]*?</section>\s*<section class="section">[\s\S]*?</section>)'
        )
        if old_us:
            us_section = old_us
            print("  [!] 美股区块本次为空，保留旧页面内容")

    execution_summary_value, execution_summary_sub = summarize_execution_strip(signal_snapshots, trades)
    html = html.replace("__EXECUTION_SUMMARY_VALUE__", execution_summary_value)
    html = html.replace("__EXECUTION_SUMMARY_SUB__", execution_summary_sub)

    # 在A股footer前插入美股部分
    html = html.replace(footer_marker, us_section + '\n' + footer_marker)

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
        if old_page_html:
            # 匹配期权 section 的整块 (包括开头注释到 </section>)
            # 也要保留紧急横幅 (如果有)
            m = re.search(
                r'(<!-- 期权持仓 section[\s\S]*?</section>)',
                old_page_html,
            )
            if m:
                opt_html = m.group(1)
                # 同时提取紧急横幅 (如果有)
                banner_m = re.search(
                    r'(<div style="background:#cf222e[\s\S]*?</script>)',
                    old_page_html,
                )
                if banner_m:
                    opt_html = banner_m.group(1) + "\n" + opt_html
                print(f"  [+] 期权持仓 section 从旧页保留 ({len(opt_html)} 字节, 预计 Actions 环境)")

    if opt_html:
        html = html.replace(
            footer_marker,
            f'<div class="position-panel" id="option-block">{opt_html}</div>\n{footer_marker}'
        )

    review_html = render_review_section()
    html = html.replace(footer_marker, review_html + "\n" + footer_marker)

    # 真实盘 section **不嵌入公开主页** (隐私保护)
    # 如果存在 real_position_section.html, 说明本地生成了, 但 docs/ 是公开的,
    # 不能暴露真实持仓细节. 真实盘数据只在本地查看 (real_position_local.html).
    real_section_path = "real_position_section.html"
    if os.path.exists(real_section_path):
        print(f"  [!] 检测到 {real_section_path} 但**不会嵌入公开主页**以保护真实持仓隐私")

    # 写到 docs/ 多页面结构 (index 收敛为总览, dashboard_full 作为完整快照与回退底稿)
    os.makedirs("docs", exist_ok=True)
    with open("docs/dashboard_full.html", "w", encoding="utf-8") as f:
        f.write(html)

    cn_match = re.search(r'(<div class="market-block cn-block" id="cn-block">[\s\S]*?</div>)\s*<div class="market-block us-block"', html)
    cn_section = cn_match.group(1) if cn_match else ""
    us_match = re.search(r'(<div class="market-block us-block" id="us-block">[\s\S]*?</div>)\s*(<div class="position-panel"|<section class="section" id="review-block"|<div class="footer")', html)
    us_section_final = us_match.group(1) if us_match else us_section
    option_match = re.search(r'(<div class="position-panel" id="option-block">[\s\S]*?</div>)\s*(<section class="section" id="review-block"|<div class="footer")', html)
    option_block_html = option_match.group(1) if option_match else (
        f'<div class="position-panel" id="option-block">{opt_html}</div>' if opt_html else ""
    )
    review_section_match = re.search(r'(<section class="section" id="review-block">[\s\S]*?</section>)\s*<div class="footer"', html)
    review_section_html = review_section_match.group(1) if review_section_match else review_html
    summary_cards_html = extract_summary_strip_html(html)

    overview_html = build_overview_page(
        full_page_html=html,
        now=now,
        summary_cards_html=summary_cards_html,
        a_weak=a_weak,
        a_total=a_total,
        us_mid_summary=us_mid_summary,
        option_summary=option_summary,
        execution_summary_value=execution_summary_value,
        execution_summary_sub=execution_summary_sub,
    )
    with open("docs/index.html", "w", encoding="utf-8") as f:
        f.write(overview_html)
    cn_page_html = build_cn_page(html, now, cn_section)
    with open("docs/cn.html", "w", encoding="utf-8") as f:
        f.write(cn_page_html)
    us_page_html = build_us_page(html, now, us_section_final, macro_headline)
    with open("docs/us.html", "w", encoding="utf-8") as f:
        f.write(us_page_html)
    options_page_html = build_options_page(
        html,
        now,
        option_block_html,
        option_summary,
        execution_summary_value,
        execution_summary_sub,
        macro_headline,
    )
    with open("docs/options.html", "w", encoding="utf-8") as f:
        f.write(options_page_html)
    strategy_page_html = build_strategy_page(
        html,
        now,
        signal_snapshots,
        execution_summary_value,
        execution_summary_sub,
    )
    with open("docs/strategy.html", "w", encoding="utf-8") as f:
        f.write(strategy_page_html)
    review_page_html = build_review_page(
        html,
        now,
        review_section_html,
        option_summary,
        execution_summary_value,
        execution_summary_sub,
        macro_headline,
        a_weak,
        a_total,
    )
    with open("docs/review.html", "w", encoding="utf-8") as f:
        f.write(review_page_html)
    print(f"\n总览页已生成: docs/index.html ({len(overview_html)} 字节)")
    print(f"A股子页已生成: docs/cn.html ({len(cn_page_html)} 字节)")
    print(f"美股子页已生成: docs/us.html ({len(us_page_html)} 字节)")
    print(f"策略子页已生成: docs/strategy.html ({len(strategy_page_html)} 字节)")
    print(f"期权子页已生成: docs/options.html ({len(options_page_html)} 字节)")
    print(f"复盘子页已生成: docs/review.html ({len(review_page_html)} 字节)")
    print(f"完整快照已生成: docs/dashboard_full.html ({len(html)} 字节)")
    print("下一步: git add docs && git commit && git push")


if __name__ == "__main__":
    args = parse_args()
    try:
        generate(allow_partial=args.allow_partial)
    except Exception as e:
        print(f"\n[FAIL] 页面生成失败: {e}", file=sys.stderr)
        sys.exit(1)
