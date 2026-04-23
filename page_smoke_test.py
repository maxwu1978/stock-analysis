"""Smoke-test generated multi-page site locally or against GitHub Pages."""

from __future__ import annotations

import argparse
from pathlib import Path

import requests


ROOT = Path(__file__).parent
DOCS = ROOT / "docs"
REMOTE_BASE = "https://maxwu1978.github.io/stock-analysis"

PAGE_MARKERS = {
    "index.html": ["Execution Pulse", "./cn.html", "./us.html", "./strategy.html", "./options.html", "./review.html"],
    "cn.html": ['id="cn-quote"', 'id="cn-trend"', 'id="cn-tech"', 'id="cn-fund"', "CN Macro Window"],
    "us.html": ['id="us-quote"', 'id="us-trend"', 'id="us-tech"', 'id="us-fund"', "Macro Overlay"],
    "strategy.html": ['id="strategy-actionable"', 'id="strategy-watchlist"', "Actionable Signals", "Watchlist Only"],
    "options.html": ['id="option-block"', "Option Positions", "./index.html"],
    "review.html": ['id="review-block"', "Execution <em>Review</em>", "./index.html"],
    "dashboard_full.html": ['id="cn-block"', 'id="us-block"', 'id="option-block"', 'id="review-block"'],
}


def load_html(name: str, remote: bool) -> str:
    if remote:
        url = f"{REMOTE_BASE}/{name}" if name != "index.html" else f"{REMOTE_BASE}/"
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        return resp.text
    return (DOCS / name).read_text(encoding="utf-8")


def run(remote: bool) -> None:
    print("\n══ Page Smoke Test ══")
    print(f"target: {'remote' if remote else 'local'}")
    missing_pages: list[tuple[str, list[str]]] = []
    for page, markers in PAGE_MARKERS.items():
        html = load_html(page, remote)
        missing = [m for m in markers if m not in html]
        if missing:
            missing_pages.append((page, missing))

    if missing_pages:
        print("缺失标记:")
        for page, markers in missing_pages:
            print(f"  [{page}]")
            for marker in markers:
                print(f"    - {marker}")
        raise SystemExit(1)

    print("所有多页关键标记齐全")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Smoke-test generated multi-page site")
    parser.add_argument("--remote", action="store_true", help="Check GitHub Pages instead of local docs/")
    args = parser.parse_args()
    run(remote=args.remote)
