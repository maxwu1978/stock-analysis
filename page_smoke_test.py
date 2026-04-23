"""Smoke-test generated page locally or against GitHub Pages."""

from __future__ import annotations

import argparse
from pathlib import Path

import requests


ROOT = Path(__file__).parent
LOCAL_PAGE = ROOT / "docs/index.html"
REMOTE_URL = "https://maxwu1978.github.io/stock-analysis/"
REQUIRED_MARKERS = [
    'id="cn-block"',
    'id="cn-trend"',
    'id="us-block"',
    'id="us-trend"',
    'id="option-block"',
    'id="review-block"',
    "CN Macro Window",
    "Macro Overlay",
    "Execution Pulse",
]


def load_html(remote: bool) -> str:
    if remote:
        resp = requests.get(REMOTE_URL, timeout=20)
        resp.raise_for_status()
        return resp.text
    return LOCAL_PAGE.read_text(encoding="utf-8")


def run(remote: bool) -> None:
    html = load_html(remote)
    missing = [m for m in REQUIRED_MARKERS if m not in html]
    print("\n══ Page Smoke Test ══")
    print(f"target: {'remote' if remote else 'local'}")
    if missing:
        print("缺失标记:")
        for m in missing:
            print(f"  - {m}")
        raise SystemExit(1)
    print("所有关键标记齐全")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Smoke-test generated page sections")
    parser.add_argument("--remote", action="store_true", help="Check GitHub Pages instead of local docs/index.html")
    args = parser.parse_args()
    run(remote=args.remote)
