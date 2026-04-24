#!/usr/bin/env python3
"""Fast-refresh the US quote board in generated static pages.

The full dashboard generation can be delayed by history/fundamental providers.
This script updates only the US quote section so GitHub Pages can publish fresh
prices even when the slower analysis pass is skipped or times out.
"""

from __future__ import annotations

import concurrent.futures
import argparse
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

from fetch_us import US_STOCKS


DOCS = [Path("docs/us.html"), Path("docs/dashboard_full.html")]
YAHOO_CHART_URLS = [
    "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}",
    "https://query2.finance.yahoo.com/v8/finance/chart/{ticker}",
]
HEADERS = {"User-Agent": "Mozilla/5.0"}


@dataclass(frozen=True)
class Quote:
    ticker: str
    name: str
    price: float
    prev_close: float
    volume: float
    high: float
    low: float
    quote_time: str

    @property
    def change_pct(self) -> float:
        return ((self.price - self.prev_close) / self.prev_close * 100) if self.prev_close else 0.0


def fmt_volume(value: float) -> str:
    return f"{value / 1e6:.1f}M" if value >= 1e6 else f"{value:,.0f}"


def chg_td(value: float) -> str:
    sign = "+" if value >= 0 else ""
    cls = "up" if value >= 0 else "down"
    return f'<td class="{cls}">{sign}{value:.2f}%</td>'


def quote_time_text(timestamp: int | float | None) -> str:
    if not timestamp:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return datetime.fromtimestamp(float(timestamp), timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def fetch_chart_quote(ticker: str) -> Quote:
    last_error: Exception | None = None
    payload = None
    for url in YAHOO_CHART_URLS:
        try:
            response = requests.get(
                url.format(ticker=ticker),
                params={"range": "1d", "interval": "1m"},
                headers=HEADERS,
                timeout=12,
            )
            response.raise_for_status()
            payload = response.json()
            break
        except Exception as exc:
            last_error = exc
    if payload is None:
        raise RuntimeError(f"{ticker}: yahoo chart failed: {last_error}")

    result = (payload.get("chart") or {}).get("result") or []
    if not result:
        raise RuntimeError(f"{ticker}: empty yahoo chart result")

    meta = result[0].get("meta") or {}
    price = float(meta.get("regularMarketPrice") or 0)
    prev = float(meta.get("chartPreviousClose") or meta.get("previousClose") or 0)
    if not price or not prev:
        raise RuntimeError(f"{ticker}: missing price or previous close")

    return Quote(
        ticker=ticker,
        name=US_STOCKS[ticker],
        price=price,
        prev_close=prev,
        volume=float(meta.get("regularMarketVolume") or 0),
        high=float(meta.get("regularMarketDayHigh") or price),
        low=float(meta.get("regularMarketDayLow") or price),
        quote_time=quote_time_text(meta.get("regularMarketTime")),
    )


def fetch_quotes() -> list[Quote]:
    quotes: dict[str, Quote] = {}
    errors: list[str] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(fetch_chart_quote, ticker): ticker for ticker in US_STOCKS}
        try:
            completed = concurrent.futures.as_completed(futures, timeout=45)
            for future in completed:
                ticker = futures[future]
                try:
                    quotes[ticker] = future.result()
                except Exception as exc:
                    errors.append(f"{ticker}: {exc}")
        except concurrent.futures.TimeoutError:
            pending = [ticker for future, ticker in futures.items() if not future.done()]
            errors.append("timeout: " + ", ".join(pending))

    if errors:
        print("  [!] 部分美股行情刷新失败: " + " | ".join(errors))
    missing = [ticker for ticker in US_STOCKS if ticker not in quotes]
    if missing:
        raise RuntimeError("美股快速行情缺失: " + ", ".join(missing))
    return [quotes[ticker] for ticker in US_STOCKS]


def old_market_caps(html: str) -> dict[str, str]:
    section = extract_us_quote_section(html)
    caps: dict[str, str] = {}
    for row in re.findall(r"<tr>([\s\S]*?)</tr>", section):
        cells = re.findall(r"<td[^>]*>([\s\S]*?)</td>", row)
        if len(cells) >= 6:
            ticker = re.sub(r"<[^>]+>", "", cells[1]).strip()
            caps[ticker] = cells[5].strip()
    return caps


def extract_us_quote_section(html: str) -> str:
    match = re.search(r'(<section class="section" id="us-quote">[\s\S]*?</section>)', html)
    return match.group(1) if match else ""


def render_quote_section(quotes: list[Quote], caps: dict[str, str]) -> str:
    quote_time = max(q.quote_time for q in quotes)
    rows = []
    for q in quotes:
        cap = caps.get(q.ticker, "-")
        rows.append(
            f"<tr><td>{escape(q.name)}</td><td>{escape(q.ticker)}</td>"
            f"<td>${q.price:.2f}</td>"
            f"{chg_td(q.change_pct)}"
            f"<td>{fmt_volume(q.volume)}</td><td>{cap}</td>"
            f"<td>${q.high:.2f}</td><td>${q.low:.2f}</td></tr>"
        )
    rows_html = "\n".join(rows)
    quote_note = (
        f'<p class="note">行情源: yahoo-chart · 报价时间: {quote_time} · '
        "GitHub Pages 为静态快照，点击 Refresh Feed 会重新拉取并发布。</p>"
    )
    return f"""<section class="section" id="us-quote">
  <div class="section-head">
    <div class="section-num">№ 05</div>
    <h2>Quote <em>Board</em><span class="cn">美股行情</span></h2>
    <div class="section-meta">Realtime<br>NYSE / NASDAQ</div>
  </div>
  {quote_note}
  <div class="table-wrap">
  <table>
  <thead><tr><th>股票</th><th>代码</th><th>现价</th><th>涨跌</th><th>成交量</th><th>市值</th><th>最高</th><th>最低</th></tr></thead>
  <tbody>{rows_html}</tbody>
  </table>
  </div>
</section>"""


def refresh_file(path: Path, quotes: list[Quote], sync_text: str) -> bool:
    if not path.exists():
        print(f"  [!] 跳过缺失文件: {path}")
        return False

    html = path.read_text(encoding="utf-8")
    old_section = extract_us_quote_section(html)
    if not old_section:
        raise RuntimeError(f"{path}: 未找到美股行情区块")

    new_section = render_quote_section(quotes, old_market_caps(html))
    updated = re.sub(
        r'\n*<section class="section" id="us-quote">[\s\S]*?</section>\n*',
        f"\n{new_section}\n",
        html,
        count=1,
    )
    updated = re.sub(r"Last Sync · [^<]+", f"Last Sync · {sync_text}", updated)
    if updated == html:
        return False
    path.write_text(updated, encoding="utf-8")
    print(f"  [+] 已刷新 {path}")
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fast-refresh U.S. quote board in generated static pages")
    return parser.parse_args()


def main() -> int:
    parse_args()
    quotes = fetch_quotes()
    sync_text = datetime.now(ZoneInfo("Asia/Dubai")).strftime("%Y-%m-%d %H:%M:%S")
    changed = False
    for path in DOCS:
        changed = refresh_file(path, quotes, sync_text) or changed
    if not changed:
        print("  [=] 美股行情区块无变化")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
