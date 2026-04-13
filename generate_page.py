#!/usr/bin/env python3
"""生成静态HTML页面, 用于GitHub Pages部署"""

import os
from datetime import datetime
import pandas as pd

from fetch_data import STOCKS, fetch_realtime_quotes, fetch_all_history
from indicators import compute_all, summarize
from probability import score_trend
from fundamental import fetch_all_financials


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


def generate():
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

        prob_html += f'<tr><td>{name}</td><td>{direction_tag(hp)}</td>'
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
<title>A股技术分析</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, "PingFang SC", "Microsoft YaHei", sans-serif;
         background: #fff; color: #1f2328; padding: 20px; max-width: 1100px; margin: 0 auto; }}
  h1 {{ color: #0969da; margin-bottom: 5px; font-size: 22px; }}
  .subtitle {{ color: #656d76; font-size: 13px; margin-bottom: 20px; }}
  h2 {{ color: #0969da; font-size: 16px; margin: 25px 0 10px 0;
       border-bottom: 2px solid #d0d7de; padding-bottom: 6px; }}
  table {{ width: 100%; border-collapse: collapse; margin-bottom: 10px; font-size: 14px; }}
  th {{ background: #f6f8fa; color: #656d76; text-align: right; padding: 8px 12px;
       border-bottom: 2px solid #d0d7de; font-weight: 600; }}
  td {{ padding: 7px 12px; text-align: right; border-bottom: 1px solid #d8dee4; }}
  th:first-child, td:first-child {{ text-align: left; font-weight: 600; color: #1f2328; }}
  tr:hover {{ background: #f6f8fa; }}
  .up {{ color: #cf222e; }}
  .down {{ color: #1a7f37; }}
  .strong {{ color: #cf222e; font-weight: 700; }}
  .weak {{ color: #8c959f; }}
  .tag {{ display: inline-block; padding: 2px 8px; border-radius: 3px; font-size: 12px; font-weight: 600; }}
  .tag-up {{ background: #ffebe9; color: #cf222e; }}
  .tag-down {{ background: #dafbe1; color: #1a7f37; }}
  .tag-neutral {{ background: #f6f8fa; color: #656d76; }}
  .footer {{ margin-top: 30px; padding-top: 15px; border-top: 1px solid #d0d7de;
            color: #656d76; font-size: 12px; line-height: 1.8; }}
</style>
</head>
<body>

<h1>A股技术分析报告</h1>
<div class="subtitle">数据更新时间: {now} &nbsp;
<button class="btn" id="refreshBtn" onclick="triggerRefresh()">刷新数据</button>
<span id="refreshMsg" style="margin-left:10px;color:#656d76;font-size:13px;"></span>
</div>
<script>
async function triggerRefresh() {{
  const btn = document.getElementById('refreshBtn');
  const msg = document.getElementById('refreshMsg');
  btn.disabled = true;
  btn.textContent = '正在刷新...';
  msg.textContent = '';

  let token = localStorage.getItem('gh_token');
  if (!token) {{
    token = prompt('首次使用请输入GitHub Personal Access Token (需要repo和workflow权限):');
    if (!token) {{
      btn.disabled = false;
      btn.textContent = '刷新数据';
      return;
    }}
    localStorage.setItem('gh_token', token);
  }}

  try {{
    const r = await fetch('https://api.github.com/repos/maxwu1978/stock-analysis/actions/workflows/update-page.yml/dispatches', {{
      method: 'POST',
      headers: {{
        'Accept': 'application/vnd.github+json',
        'Authorization': 'Bearer ' + token
      }},
      body: JSON.stringify({{ref: 'main'}})
    }});
    if (r.status === 204) {{
      msg.textContent = '已触发更新, 约2分钟后自动刷新页面...';
      msg.style.color = '#1a7f37';
      setTimeout(() => location.reload(), 120000);
    }} else if (r.status === 401 || r.status === 403) {{
      localStorage.removeItem('gh_token');
      msg.textContent = 'Token无效或权限不足, 请重新点击刷新输入';
      msg.style.color = '#cf222e';
    }} else {{
      msg.textContent = '触发失败: HTTP ' + r.status;
      msg.style.color = '#cf222e';
    }}
  }} catch(e) {{
    msg.textContent = '网络错误: ' + e.message;
    msg.style.color = '#cf222e';
  }}
  btn.disabled = false;
  btn.textContent = '刷新数据';
}}
</script>

<h2>最新行情</h2>
<table>
<tr><th>股票</th><th>代码</th><th>现价</th><th>涨跌</th><th>成交额</th><th>最高</th><th>最低</th></tr>
{quote_html}
</table>

<h2>趋势概率</h2>
<div style="color:#656d76; font-size:12px; margin-bottom:8px;">
方向由30日上涨概率决定: &gt;55%偏涨, &lt;45%偏跌
</div>
<table>
<tr><th>股票</th><th>方向</th><th>5日</th><th>10日</th><th>30日</th><th>180日</th></tr>
{prob_html}
</table>

<h2>技术指标</h2>
<table>
<tr><th>股票</th><th>RSI6</th><th>MACD柱</th><th>MA5</th><th>MA20</th><th>MA60</th><th>ADX</th><th>股性</th></tr>
{tech_html}
</table>

<h2>最新财报</h2>
<table>
<tr><th>股票</th><th>报告期</th><th>ROE</th><th>营收增长</th><th>利润增长</th><th>毛利率</th><th>负债率</th></tr>
{fund_html}
</table>

<div class="footer">
模型: 25因子(17技术+8财报) x 滚动IC加权, 6年5800样本回测<br>
免责: 以上仅为统计概率, 不构成任何投资建议, 过去不代表未来
</div>

</body></html>"""

    # 写到 docs/index.html (GitHub Pages 从 docs 目录读取)
    os.makedirs("docs", exist_ok=True)
    with open("docs/index.html", "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\n页面已生成: docs/index.html ({len(html)} 字节)")
    print("下一步: git add docs && git commit && git push")


if __name__ == "__main__":
    generate()
