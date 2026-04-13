#!/usr/bin/env python3
"""美股技术分析 - Markdown表格输出"""

from datetime import datetime
import sys
import pandas as pd
from fetch_us import US_STOCKS, fetch_us_realtime, fetch_us_all_history, fetch_us_financials
from indicators import compute_all, summarize
from probability import score_trend
from fundamental import align_fundamental_to_daily


def direction_from_prob(hp):
    p30 = hp.get("30日")
    if not p30:
        return "--"
    pct = int(p30["上涨概率"].replace("%", ""))
    if pct >= 60: return "看涨"
    if pct >= 55: return "偏涨"
    if pct <= 35: return "看跌"
    if pct <= 45: return "偏跌"
    return "震荡"


def md_table(headers, rows):
    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        lines.append("| " + " | ".join(str(c) for c in row) + " |")
    return "\n".join(lines)


def main():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("_正在获取数据..._", file=sys.stderr)

    try:
        rt = fetch_us_realtime()
    except:
        rt = pd.DataFrame()
    try:
        all_fund = fetch_us_financials()
    except:
        all_fund = {}
    try:
        all_hist = fetch_us_all_history("5y")
    except:
        all_hist = {}

    print(f"## 美股技术分析报告 {now}\n")

    # 行情
    if not rt.empty:
        rows = []
        for _, r in rt.iterrows():
            chg = r.get("涨跌幅", 0)
            vol = r.get("成交额", 0)
            vol_s = f"{vol/1e6:.1f}M" if vol >= 1e6 else f"{vol:,.0f}"
            mcap = r.get("市值", 0)
            mcap_s = f"{mcap/1e12:.2f}T" if mcap >= 1e12 else f"{mcap/1e9:.0f}B"
            sign = "+" if chg >= 0 else ""
            rows.append([
                r["名称"], r["代码"],
                f"${r.get('最新价',0):.2f}",
                f"**{sign}{chg:.2f}%**",
                vol_s, mcap_s,
                f"${r.get('最高',0):.2f}",
                f"${r.get('最低',0):.2f}",
            ])
        print("### 最新行情\n")
        print(md_table(["股票", "代码", "现价", "涨跌", "成交量", "市值", "最高", "最低"], rows))

    # 分析
    prob_rows = []
    tech_rows = []
    fund_rows = []

    for ticker, df in all_hist.items():
        name = US_STOCKS[ticker]
        fund_df = all_fund.get(ticker)
        df = compute_all(df, fund_df)
        s = summarize(df)
        if not s:
            continue

        prob = score_trend(df)
        if "error" in prob:
            continue

        hp = prob.get("historical_prob", {})
        direction = direction_from_prob(hp)

        def fmt_p(d):
            if not d: return "-"
            p = int(d["上涨概率"].replace("%", ""))
            avg = d["平均收益"]
            n = d["样本数"]
            if p >= 60: return f"**{p}%** {avg} (n={n})"
            if p <= 40: return f"_{p}%_ {avg} (n={n})"
            return f"{p}% {avg} (n={n})"

        prob_rows.append([
            f"**{name}**", f"**{direction}**",
            fmt_p(hp.get("5日")), fmt_p(hp.get("10日")),
            fmt_p(hp.get("30日")), fmt_p(hp.get("180日")),
        ])

        rg = prob.get("regime", {})
        stype = {"momentum": "动量", "mean_revert": "回归", "mixed": "混合"}.get(rg.get("stock_type"), "?")
        tech_rows.append([
            name, s["RSI6"], s["MACD"], s["MA5"], s["MA20"], s["MA60"],
            f"{rg.get('adx',0):.0f}", stype,
        ])

        if fund_df is not None and not fund_df.empty:
            lt = fund_df.iloc[-1]
            rpt = lt.get("report_date")
            rpt_s = rpt.strftime("%Y-%m-%d") if hasattr(rpt, "strftime") else str(rpt)
            def fv(key, fmt=".1f"):
                v = lt.get(key)
                if pd.isna(v): return "-"
                return f"{v:{fmt}}%"
            fund_rows.append([
                name, rpt_s, fv("roe"), fv("rev_growth", "+.1f"),
                fv("profit_growth", "+.1f"), fv("gross_margin"), fv("debt_ratio"),
            ])

    print(f"\n### 趋势概率\n")
    print(md_table(["股票", "方向", "5日", "10日", "30日", "180日"], prob_rows))

    print(f"\n### 技术指标\n")
    print(md_table(["股票", "RSI6", "MACD柱", "MA5", "MA20", "MA60", "ADX", "股性"], tech_rows))

    if fund_rows:
        print(f"\n### 最新财报\n")
        print(md_table(["股票", "报告期", "ROE", "营收增长", "利润增长", "毛利率", "负债率"], fund_rows))

    print(f"\n---")
    print(f"模型: 25因子 x 滚动IC加权 | 免责: 仅为统计概率, 不构成投资建议")


if __name__ == "__main__":
    main()
