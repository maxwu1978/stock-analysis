#!/usr/bin/env python3
"""Unified local command runner for the stock-analysis project.

This wrapper intentionally keeps the existing scripts in place. It only
standardizes the commands used during daily operation and release checks.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def run(args: list[str], *, check: bool = True, display: str | None = None) -> int:
    env = os.environ.copy()
    env.setdefault("NONINTERACTIVE", "1")
    env.setdefault("PYTHONUNBUFFERED", "1")
    env.setdefault("MPLBACKEND", "Agg")
    env.setdefault("GIT_TERMINAL_PROMPT", "0")
    env.setdefault("PIP_NO_INPUT", "1")
    print("+ " + (display or " ".join(args)), flush=True)
    completed = subprocess.run(args, cwd=ROOT, env=env, check=check)
    return completed.returncode


def python_cmd(script: str, extra: list[str]) -> int:
    if not (ROOT / script).exists():
        raise SystemExit(f"missing script: {script}")
    return run([sys.executable, script, *extra])


def cmd_refresh_page(extra: list[str]) -> int:
    args = extra or ["--allow-partial"]
    return python_cmd("generate_page.py", args)


def cmd_smoke_test(extra: list[str]) -> int:
    return python_cmd("page_smoke_test.py", extra)


def cmd_scan_cn(extra: list[str]) -> int:
    return python_cmd("scan_a_opportunities.py", extra)


def cmd_refresh_options(extra: list[str]) -> int:
    return python_cmd("refresh_option_strategy.py", extra)


def cmd_factor_learn(extra: list[str]) -> int:
    return python_cmd("factor_learning.py", extra)


def cmd_factor_test(extra: list[str]) -> int:
    return python_cmd("factor_testing.py", extra)


def cmd_preflight(extra: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="manage.py preflight")
    parser.add_argument("--remote", action="store_true", help="also smoke-test GitHub Pages")
    args = parser.parse_args(extra)

    py_files = [str(path) for path in sorted(ROOT.glob("*.py"))]
    run([sys.executable, "-m", "compileall", "-q", *py_files], display=f"{sys.executable} -m compileall -q *.py")
    run([sys.executable, "page_smoke_test.py"])
    if args.remote:
        run([sys.executable, "page_smoke_test.py", "--remote"])
    run(["git", "diff", "--check"])
    return 0


COMMANDS = {
    "refresh-page": (cmd_refresh_page, "Generate docs pages. Defaults to --allow-partial."),
    "smoke-test": (cmd_smoke_test, "Check generated pages locally or with --remote."),
    "scan-cn": (cmd_scan_cn, "Run A-share opportunity scan."),
    "refresh-options": (cmd_refresh_options, "Refresh option strategy output."),
    "factor-learn": (cmd_factor_learn, "Run candidate factor learning."),
    "factor-test": (cmd_factor_test, "Run candidate factor testing."),
    "preflight": (cmd_preflight, "Run release checks."),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stock-analysis project command runner")
    parser.add_argument("command", choices=sorted(COMMANDS))
    parser.add_argument("args", nargs=argparse.REMAINDER, help="arguments passed to the selected command")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    handler, _ = COMMANDS[args.command]
    raise SystemExit(handler(args.args))


if __name__ == "__main__":
    main()
