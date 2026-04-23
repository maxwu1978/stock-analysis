#!/usr/bin/env python3
"""Render a human-readable factor lab summary from summary CSVs and promotion queue."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent


@dataclass(slots=True)
class CandidateItem:
    market: str
    factor: str
    family: str
    decision: str
    reason: str
    best_abs_rankic: float | None
    mean_abs_rankic: float | None
    stable_count: int
    mean_consistency: float | None
    best_orthogonality: float | None
    max_coverage: float | None
    closest_active_factor: str | None
    quality_score: float | None
    best_horizon: int | None = None
    recommended_action: str | None = None


@dataclass(slots=True)
class MarketDigest:
    market: str
    promote: list[CandidateItem]
    watch: list[CandidateItem]
    reject_duplicate: list[CandidateItem]
    reject: list[CandidateItem]
    total: int


@dataclass(slots=True)
class LabReport:
    generated_at: str | None
    thresholds: dict[str, Any]
    markets: dict[str, MarketDigest]


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except Exception:
        return None
    if pd.isna(number):
        return None
    return number


def _safe_int(value: Any) -> int:
    try:
        number = int(value)
    except Exception:
        return 0
    return number


def _fmt_num(value: float | None, digits: int = 4, suffix: str = "") -> str:
    if value is None:
        return "-"
    return f"{value:.{digits}f}{suffix}"


def _fmt_pct(value: float | None, digits: int = 1) -> str:
    if value is None:
        return "-"
    return f"{value:.{digits}f}%"


def _market_label(market: str) -> str:
    return {"a": "A股", "us": "美股"}.get(market, market.upper())


def _decision_rank(item: CandidateItem) -> tuple[int, float]:
    mapping = {
        "PROMOTE_TO_TRIAL": 0,
        "WATCH": 1,
        "REJECT_DUPLICATE": 2,
        "REJECT": 3,
    }
    return mapping.get(item.decision, 9), -(item.quality_score or 0.0)


def _build_item(raw: dict[str, Any], fallback_market: str) -> CandidateItem:
    closest = raw.get("closest_active_factor")
    if closest in ("", "-", None) or pd.isna(closest):
        closest = None
    return CandidateItem(
        market=str(raw.get("market") or fallback_market),
        factor=str(raw.get("factor") or "-"),
        family=str(raw.get("family") or "-"),
        decision=str(raw.get("decision") or raw.get("best_decision") or "-"),
        reason=str(raw.get("reason") or raw.get("recommended_action") or ""),
        best_abs_rankic=_safe_float(raw.get("best_abs_rankic")),
        mean_abs_rankic=_safe_float(raw.get("mean_abs_rankic")),
        stable_count=_safe_int(raw.get("stable_count")),
        mean_consistency=_safe_float(raw.get("mean_consistency")),
        best_orthogonality=_safe_float(raw.get("best_orthogonality")),
        max_coverage=_safe_float(raw.get("max_coverage")),
        closest_active_factor=closest,
        quality_score=_safe_float(raw.get("quality_score")),
        best_horizon=_safe_int(raw.get("best_horizon")) if raw.get("best_horizon") is not None else None,
        recommended_action=None if raw.get("recommended_action") is None else str(raw.get("recommended_action")),
    )


def _read_summary(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"缺少 summary 文件: {path}")
    return pd.read_csv(path)


def _read_queue(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"缺少 promotion queue 文件: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _market_digest_from_queue(market: str, market_queue: dict[str, Any], summary_df: pd.DataFrame) -> MarketDigest:
    detailed_items: dict[str, CandidateItem] = {}
    for raw in market_queue.get("all", []):
        item = _build_item(raw, market)
        detailed_items[item.factor] = item

    for _, row in summary_df.iterrows():
        factor = str(row["factor"])
        if factor in detailed_items:
            continue
        detailed_items[factor] = _build_item(row.to_dict(), market)

    items = sorted(detailed_items.values(), key=_decision_rank)
    promote = [item for item in items if item.decision == "PROMOTE_TO_TRIAL"]
    watch = [item for item in items if item.decision == "WATCH"]
    reject_duplicate = [item for item in items if item.decision == "REJECT_DUPLICATE"]
    reject = [item for item in items if item.decision == "REJECT"]
    return MarketDigest(
        market=market,
        promote=promote,
        watch=watch,
        reject_duplicate=reject_duplicate,
        reject=reject,
        total=len(items),
    )


def build_lab_report(
    *,
    a_summary_path: Path,
    us_summary_path: Path,
    queue_path: Path,
) -> LabReport:
    a_summary = _read_summary(a_summary_path)
    us_summary = _read_summary(us_summary_path)
    queue = _read_queue(queue_path)
    markets = queue.get("markets", {})
    return LabReport(
        generated_at=queue.get("generated_at"),
        thresholds=dict(queue.get("thresholds", {})),
        markets={
            "a": _market_digest_from_queue("a", dict(markets.get("a", {})), a_summary),
            "us": _market_digest_from_queue("us", dict(markets.get("us", {})), us_summary),
        },
    )


def load_factor_lab_bundle(
    *,
    a_summary_path: Path | None = None,
    us_summary_path: Path | None = None,
    queue_path: Path | None = None,
) -> dict[str, Any]:
    a_summary_path = a_summary_path or ROOT / "factor_candidate_a_summary.csv"
    us_summary_path = us_summary_path or ROOT / "factor_candidate_us_summary.csv"
    queue_path = queue_path or ROOT / "factor_promotion_queue.json"
    a_summary = _read_summary(a_summary_path) if a_summary_path.exists() else pd.DataFrame()
    us_summary = _read_summary(us_summary_path) if us_summary_path.exists() else pd.DataFrame()
    queue = _read_queue(queue_path) if queue_path.exists() else {}
    return {
        "a_summary": a_summary,
        "us_summary": us_summary,
        "queue": queue,
    }


def _terminal_market_table(items: list[CandidateItem], *, kind: str, limit: int) -> str:
    if not items:
        return f"  {kind}: 无"
    rows = [f"  {kind}:"]
    for item in items[:limit]:
        overlap = item.closest_active_factor or "-"
        horizon = f"{item.best_horizon}D" if item.best_horizon else "-"
        rows.append(
            "    - "
            f"{item.factor:<22} {item.family:<10} |RankIC|={_fmt_num(item.best_abs_rankic)} "
            f"cover={_fmt_pct(item.max_coverage)} orth={_fmt_num(item.best_orthogonality)} "
            f"h={horizon} overlap={overlap}"
        )
        if item.reason:
            rows.append(f"      理由: {item.reason}")
    return "\n".join(rows)


def render_terminal_report(report: LabReport, *, watch_limit: int = 3, reject_limit: int = 2) -> str:
    lines = []
    lines.append("═" * 92)
    lines.append("Factor Lab Research Summary")
    lines.append("═" * 92)
    if report.generated_at:
        lines.append(f"生成时间: {report.generated_at}")
    if report.thresholds:
        lines.append(
            "阈值: "
            f"promote_rankic>={report.thresholds.get('min_promote_rankic', '-')}, "
            f"watch_rankic>={report.thresholds.get('min_watch_rankic', '-')}, "
            f"coverage>={report.thresholds.get('min_coverage', '-')}"
        )

    total_promote = sum(len(digest.promote) for digest in report.markets.values())
    total_watch = sum(len(digest.watch) for digest in report.markets.values())
    total_dup = sum(len(digest.reject_duplicate) for digest in report.markets.values())
    total_reject = sum(len(digest.reject) for digest in report.markets.values())
    lines.append(
        f"总览: 晋升队列 {total_promote} | 观察名单 {total_watch} | 重复淘汰 {total_dup} | 直接淘汰 {total_reject}"
    )
    if total_promote == 0:
        lines.append("当前没有足够强的新因子进入试验晋升队列，重点应放在观察名单和重复因子清理。")

    for market in ("a", "us"):
        digest = report.markets[market]
        lines.append("")
        lines.append(f"[{_market_label(market)}] total={digest.total}")
        lines.append(_terminal_market_table(digest.promote, kind="晋升队列", limit=watch_limit))
        lines.append(_terminal_market_table(digest.watch, kind="观察名单", limit=watch_limit))
        lines.append(_terminal_market_table(digest.reject_duplicate, kind="重复淘汰", limit=reject_limit))
        lines.append(_terminal_market_table(digest.reject, kind="直接淘汰", limit=reject_limit))

    lines.append("")
    lines.append("建议:")
    if total_promote:
        lines.append("  1. 将晋升队列因子纳入 trial backtest，不直接改 active 因子池。")
    else:
        lines.append("  1. 继续跟踪观察名单，不要直接把候选因子接进主模型。")
    lines.append("  2. 对 REJECT_DUPLICATE 的候选，优先停掉同类变体试验。")
    lines.append("  3. 后续若挂到 research.html / review.html，可先展示观察名单与重复淘汰原因。")
    return "\n".join(lines)


def _markdown_table(items: list[CandidateItem], *, columns: list[tuple[str, str]], empty_text: str) -> str:
    if not items:
        return empty_text
    header = "| " + " | ".join(title for title, _ in columns) + " |"
    sep = "| " + " | ".join(["---"] * len(columns)) + " |"
    rows = [header, sep]
    for item in items:
        values = []
        for _, key in columns:
            if key == "factor":
                values.append(item.factor)
            elif key == "family":
                values.append(item.family)
            elif key == "best_abs_rankic":
                values.append(_fmt_num(item.best_abs_rankic))
            elif key == "best_orthogonality":
                values.append(_fmt_num(item.best_orthogonality))
            elif key == "max_coverage":
                values.append(_fmt_pct(item.max_coverage))
            elif key == "closest_active_factor":
                values.append(item.closest_active_factor or "-")
            elif key == "reason":
                values.append(item.reason or "-")
            elif key == "best_horizon":
                values.append(f"{item.best_horizon}D" if item.best_horizon else "-")
            else:
                values.append("-")
        rows.append("| " + " | ".join(values) + " |")
    return "\n".join(rows)


def render_markdown_report(report: LabReport, *, watch_limit: int = 5, reject_limit: int = 3) -> str:
    lines = ["# Factor Lab Research Summary", ""]
    if report.generated_at:
        lines.append(f"- 生成时间: `{report.generated_at}`")
    total_promote = sum(len(d.promote) for d in report.markets.values())
    total_watch = sum(len(d.watch) for d in report.markets.values())
    total_dup = sum(len(d.reject_duplicate) for d in report.markets.values())
    total_reject = sum(len(d.reject) for d in report.markets.values())
    lines.append(
        f"- 总览: 晋升队列 `{total_promote}`，观察名单 `{total_watch}`，重复淘汰 `{total_dup}`，直接淘汰 `{total_reject}`"
    )
    lines.append("- 说明: 当前摘要只用于研究，不会自动修改 `FACTOR_COLS / US_FACTOR_COLS`。")
    lines.append("")

    for market in ("a", "us"):
        digest = report.markets[market]
        lines.append(f"## {_market_label(market)}")
        lines.append("")
        lines.append(f"- 候选总数: `{digest.total}`")
        lines.append(f"- 晋升队列: `{len(digest.promote)}`")
        lines.append(f"- 观察名单: `{len(digest.watch)}`")
        lines.append(f"- 重复淘汰: `{len(digest.reject_duplicate)}`")
        lines.append(f"- 直接淘汰: `{len(digest.reject)}`")
        lines.append("")

        lines.append("### 观察名单")
        lines.append(
            _markdown_table(
                digest.watch[:watch_limit],
                columns=[
                    ("因子", "factor"),
                    ("家族", "family"),
                    ("Abs RankIC", "best_abs_rankic"),
                    ("正交性", "best_orthogonality"),
                    ("覆盖率", "max_coverage"),
                    ("最接近 active", "closest_active_factor"),
                    ("说明", "reason"),
                ],
                empty_text="无",
            )
        )
        lines.append("")
        lines.append("### 重复/淘汰")
        lines.append(
            _markdown_table(
                (digest.reject_duplicate + digest.reject)[:reject_limit],
                columns=[
                    ("因子", "factor"),
                    ("家族", "family"),
                    ("Abs RankIC", "best_abs_rankic"),
                    ("最接近 active", "closest_active_factor"),
                    ("说明", "reason"),
                ],
                empty_text="无",
            )
        )
        lines.append("")

    lines.append("## 研究建议")
    lines.append("")
    if total_promote == 0:
        lines.append("- 当前没有足够强的新因子进入试验晋升队列，重点应继续跟踪观察名单。")
    else:
        lines.append("- 仅对晋升队列做 trial backtest，不直接进入 active 因子池。")
    lines.append("- `REJECT_DUPLICATE` 说明候选与现有 active 因子高度重叠，应优先停掉变体试验。")
    lines.append("- 该摘要可以直接挂到 `research.html` 或 `review.html`，作为研究诊断区。")
    return "\n".join(lines)


def _decision_tag(item: CandidateItem) -> str:
    if item.decision == "PROMOTE_TO_TRIAL":
        return '<span class="tag tag-up">试验晋升</span>'
    if item.decision == "WATCH":
        return '<span class="tag tag-neutral">观察</span>'
    if item.decision == "REJECT_DUPLICATE":
        return '<span class="tag tag-down">重复</span>'
    return '<span class="tag tag-down">淘汰</span>'


def _summary_card(label: str, value: str, sub: str) -> str:
    return (
        "<article class=\"summary-card\">"
        f"<div class=\"label\">{label}</div>"
        f"<div class=\"value\">{value}</div>"
        f"<div class=\"sub\">{sub}</div>"
        "</article>"
    )


def _html_rows(items: list[CandidateItem], empty_text: str) -> str:
    if not items:
        return f'<tr><td colspan="7">{empty_text}</td></tr>'
    rows = []
    for item in items:
        rows.append(
            "<tr>"
            f"<td><strong>{item.factor}</strong><br><small>{item.family}</small></td>"
            f"<td>{_decision_tag(item)}</td>"
            f"<td>{_fmt_num(item.best_abs_rankic)}</td>"
            f"<td>{_fmt_num(item.best_orthogonality)}</td>"
            f"<td>{_fmt_pct(item.max_coverage)}</td>"
            f"<td>{item.closest_active_factor or '-'}</td>"
            f"<td>{item.reason or item.recommended_action or '-'}</td>"
            "</tr>"
        )
    return "\n".join(rows)


def _market_block_html(digest: MarketDigest, number: str) -> str:
    market_title = _market_label(digest.market)
    top_watch = digest.watch[:5]
    top_reject = (digest.reject_duplicate + digest.reject)[:5]
    note = (
        "当前没有足够证据进入试验晋升队列，重点应继续观察局部有效且正交性尚可的候选。"
        if not digest.promote
        else "当前已有候选可进入试验回测池，但仍不应直接改 active 因子池。"
    )
    return f"""
<div class="market-block {'cn-block' if digest.market == 'a' else 'us-block'}" id="factor-lab-{digest.market}">
  <div class="section-head">
    <div class="section-num">№ {number}</div>
    <h2>{market_title} <em>Factor Lab</em><span class="cn">候选因子摘要</span></h2>
    <div class="section-meta">Candidate Research<br>Promotion Queue</div>
  </div>
  <p class="note">{note}</p>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>观察名单</th>
          <th>状态</th>
          <th>|RankIC|</th>
          <th>正交性</th>
          <th>覆盖率</th>
          <th>最接近 active</th>
          <th>研究结论</th>
        </tr>
      </thead>
      <tbody>{_html_rows(top_watch, "暂无观察名单")}</tbody>
    </table>
  </div>
  <div class="table-wrap" style="margin-top:18px;">
    <table>
      <thead>
        <tr>
          <th>重复/淘汰</th>
          <th>状态</th>
          <th>|RankIC|</th>
          <th>正交性</th>
          <th>覆盖率</th>
          <th>最接近 active</th>
          <th>研究结论</th>
        </tr>
      </thead>
      <tbody>{_html_rows(top_reject, "暂无重复或淘汰记录")}</tbody>
    </table>
  </div>
</div>
"""


def render_html_fragment(report: LabReport) -> str:
    total_promote = sum(len(d.promote) for d in report.markets.values())
    total_watch = sum(len(d.watch) for d in report.markets.values())
    total_dup = sum(len(d.reject_duplicate) for d in report.markets.values())
    total_reject = sum(len(d.reject) for d in report.markets.values())
    generated = report.generated_at or "-"
    summary_cards = "\n".join(
        [
            _summary_card("Promotion Queue", str(total_promote), "当前可进入 trial backtest 的候选"),
            _summary_card("Watchlist", str(total_watch), "局部有效但证据还不够强"),
            _summary_card("Duplicate Rejects", str(total_dup), "与现有 active 因子高度重叠"),
            _summary_card("Hard Rejects", str(total_reject), "覆盖不足或信号强度不够"),
        ]
    )
    threshold_note = (
        f"当前门槛: promote_rankic≥{report.thresholds.get('min_promote_rankic', '-')}, "
        f"watch_rankic≥{report.thresholds.get('min_watch_rankic', '-')}, "
        f"coverage≥{report.thresholds.get('min_coverage', '-')}"
    )
    return f"""<!-- Factor lab research fragment: ready to embed into research.html or review.html -->
<section class="section" id="factor-lab-report">
  <div class="section-head">
    <div class="section-num">№ 11</div>
    <h2>Factor Lab <em>Research</em><span class="cn">候选因子研究摘要</span></h2>
    <div class="section-meta">Generated<br>{generated}</div>
  </div>
  <p class="note">
    该摘要只用于研究层，不会自动修改 active 因子池或交易动作。{threshold_note}
  </p>
</section>
<section class="summary-strip">
{summary_cards}
</section>
{_market_block_html(report.markets['a'], "11A")}
{_market_block_html(report.markets['us'], "11B")}
"""


def render_factor_lab_section_html(bundle: dict[str, Any]) -> str:
    queue = bundle.get("queue") or {}
    if not queue:
        return """
<section class="section" id="factor-lab-block">
  <div class="section-head">
    <div class="section-num">№ 11</div>
    <h2>Factor <em>Lab</em><span class="cn">候选因子研究</span></h2>
    <div class="section-meta">Research<br>Promotion Queue</div>
  </div>
  <p class="note">当前还没有候选因子研究输出。请先运行 factor_lab.py 和 factor_promotion.py。</p>
</section>
"""

    report = build_lab_report(
        a_summary_path=ROOT / "factor_candidate_a_summary.csv",
        us_summary_path=ROOT / "factor_candidate_us_summary.csv",
        queue_path=ROOT / "factor_promotion_queue.json",
    )
    html_fragment = render_html_fragment(report)
    return html_fragment.replace('id="factor-lab-report"', 'id="factor-lab-block"', 1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render human-readable factor lab summary")
    parser.add_argument("--a-summary", type=Path, default=ROOT / "factor_candidate_a_summary.csv")
    parser.add_argument("--us-summary", type=Path, default=ROOT / "factor_candidate_us_summary.csv")
    parser.add_argument("--queue", type=Path, default=ROOT / "factor_promotion_queue.json")
    parser.add_argument("--markdown-out", type=Path)
    parser.add_argument("--html-out", type=Path)
    parser.add_argument("--watch-limit", type=int, default=5)
    parser.add_argument("--reject-limit", type=int, default=3)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report = build_lab_report(
        a_summary_path=args.a_summary,
        us_summary_path=args.us_summary,
        queue_path=args.queue,
    )
    print(render_terminal_report(report, watch_limit=args.watch_limit, reject_limit=args.reject_limit))

    if args.markdown_out:
        markdown = render_markdown_report(report, watch_limit=args.watch_limit, reject_limit=args.reject_limit)
        args.markdown_out.write_text(markdown, encoding="utf-8")
        print(f"\n输出 Markdown: {args.markdown_out}")

    if args.html_out:
        html = render_html_fragment(report)
        args.html_out.write_text(html, encoding="utf-8")
        print(f"输出 HTML 片段: {args.html_out}")


if __name__ == "__main__":
    main()
