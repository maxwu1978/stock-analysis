#!/usr/bin/env python3
"""Refresh local option strategy section without rebuilding the full dashboard."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from generate_page import collect_option_strategy_signals, render_option_setups_section


ROOT = Path(__file__).resolve().parent
DEFAULT_STRATEGY_PAGE = ROOT / "docs" / "strategy.html"


def _run_monitor(timeout: int) -> tuple[bool, str]:
    try:
        run = subprocess.run(
            [sys.executable, "option_monitor.py", "--quiet"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return False, f"option_monitor.py timeout after {timeout}s"
    except Exception as exc:
        return False, str(exc)

    if run.returncode != 0:
        msg = (run.stderr or run.stdout or "").strip()
        return False, msg[:240]
    return True, "option_section.html refreshed"


def _replace_option_summary(html: str, option_signals: list[dict]) -> str:
    setup_count = sum(1 for row in option_signals if row.get("strength") in {"强机会", "弱机会"})
    pattern = re.compile(
        r'(<article class="summary-card">\s*<div class="label">Option Setup</div>\s*'
        r'<div class="value">)(.*?)(</div>\s*<div class="sub">)(.*?)(</div>\s*</article>)',
        re.S,
    )
    return pattern.sub(
        rf"\g<1>{setup_count}\g<3>本地刷新 · 全池期权扫描\g<5>",
        html,
        count=1,
    )


def refresh_strategy_page(path: Path, *, skip_monitor: bool, monitor_timeout: int) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"strategy page not found: {path}")

    monitor_status = "skipped"
    if not skip_monitor:
        ok, msg = _run_monitor(timeout=monitor_timeout)
        monitor_status = msg if ok else f"monitor failed: {msg}"

    option_signals = collect_option_strategy_signals()
    new_section = render_option_setups_section(option_signals)

    html = path.read_text(encoding="utf-8")
    section_pattern = re.compile(r'<section class="section" id="strategy-options">[\s\S]*?</section>')
    html, count = section_pattern.subn(new_section.strip(), html, count=1)
    if count != 1:
        raise RuntimeError("strategy-options section was not found or was ambiguous")

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    html = re.sub(r"Last Sync · [^<]+", f"Last Sync · {now}", html, count=1)
    html = _replace_option_summary(html, option_signals)
    path.write_text(html, encoding="utf-8")

    return {
        "path": str(path),
        "updated_at": now,
        "monitor_status": monitor_status,
        "total": len(option_signals),
        "opportunities": sum(1 for row in option_signals if row.get("strength") in {"强机会", "弱机会"}),
        "holdings": sum(1 for row in option_signals if row.get("strength") == "持仓管理"),
        "no_opportunity": sum(1 for row in option_signals if row.get("strength") == "无机会"),
        "signals": option_signals,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh local option strategy scan in docs/strategy.html")
    parser.add_argument("--strategy-page", type=Path, default=DEFAULT_STRATEGY_PAGE)
    parser.add_argument("--skip-monitor", action="store_true", help="Do not refresh option_section.html first")
    parser.add_argument("--monitor-timeout", type=int, default=60)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = refresh_strategy_page(
        args.strategy_page,
        skip_monitor=args.skip_monitor,
        monitor_timeout=args.monitor_timeout,
    )

    print("\n══ Local Option Strategy Refresh ══")
    print(f"page: {result['path']}")
    print(f"time: {result['updated_at']}")
    print(f"monitor: {result['monitor_status']}")
    print(
        f"signals: {result['opportunities']} opportunity / "
        f"{result['holdings']} holding / {result['no_opportunity']} no-opportunity"
    )
    for row in result["signals"]:
        if row.get("strength") in {"强机会", "弱机会", "持仓管理"}:
            print(f"- {row.get('label')}: {row.get('strength')} · {row.get('action')} · {row.get('plan')}")


if __name__ == "__main__":
    main()
