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


def cmd_validate_a(extra: list[str]) -> int:
    return python_cmd("a_share_signal_validation_2y.py", extra)


def cmd_capital_flow_backtest(extra: list[str]) -> int:
    return python_cmd("cn_capital_flow_backtest.py", extra)


def cmd_industry_heat(extra: list[str]) -> int:
    return python_cmd("industry_heat.py", extra)


def cmd_kronos_reference(extra: list[str]) -> int:
    return python_cmd("build_kronos_reference.py", extra)


def cmd_option_signal_review(extra: list[str]) -> int:
    return python_cmd("historical_option_signal_review.py", extra)


def cmd_option_pnl_review(extra: list[str]) -> int:
    return python_cmd("historical_option_pnl_review.py", extra)


def cmd_option_account_sim(extra: list[str]) -> int:
    return python_cmd("historical_option_account_sim.py", extra)


def cmd_import_option_chains(extra: list[str]) -> int:
    return python_cmd("import_option_chain_data.py", extra)


def cmd_fetch_option_chains(extra: list[str]) -> int:
    return python_cmd("fetch_dolthub_option_chains.py", extra)


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


def cmd_list_commands(extra: list[str]) -> int:
    if extra:
        raise SystemExit("list-commands does not accept extra arguments")
    for name, (_, description) in sorted(COMMANDS.items()):
        print(f"{name:<24} {description}")
    return 0


COMMANDS = {
    "capital-flow-backtest": (cmd_capital_flow_backtest, "Backtest CN capital-flow intent labels."),
    "factor-learn": (cmd_factor_learn, "Run candidate factor learning."),
    "factor-test": (cmd_factor_test, "Run candidate factor testing."),
    "fetch-option-chains": (cmd_fetch_option_chains, "Fetch needed historical option-chain slices."),
    "import-option-chains": (cmd_import_option_chains, "Import third-party option-chain CSV snapshots."),
    "industry-heat": (cmd_industry_heat, "Run industry heat and potential analysis."),
    "kronos-reference": (cmd_kronos_reference, "Build research-only Kronos reference snapshot."),
    "list-commands": (cmd_list_commands, "Print available project commands."),
    "option-account-sim": (cmd_option_account_sim, "Simulate account equity from option trades."),
    "option-pnl-review": (cmd_option_pnl_review, "Review historical option proxy PnL."),
    "option-signal-review": (cmd_option_signal_review, "Replay historical strong/weak option signals."),
    "refresh-page": (cmd_refresh_page, "Generate docs pages. Defaults to --allow-partial."),
    "refresh-options": (cmd_refresh_options, "Refresh option strategy output."),
    "scan-cn": (cmd_scan_cn, "Run A-share opportunity scan."),
    "smoke-test": (cmd_smoke_test, "Check generated pages locally or with --remote."),
    "validate-a": (cmd_validate_a, "Validate current A-share signal rules over recent years."),
    "preflight": (cmd_preflight, "Run release checks."),
}


def parse_args() -> argparse.Namespace:
    command_lines = "\n".join(f"  {name:<24} {description}" for name, (_, description) in sorted(COMMANDS.items()))
    parser = argparse.ArgumentParser(
        description="Stock-analysis project command runner",
        epilog=f"Available commands:\n{command_lines}",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("command", choices=sorted(COMMANDS))
    parser.add_argument("args", nargs=argparse.REMAINDER, help="arguments passed to the selected command")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    handler, _ = COMMANDS[args.command]
    raise SystemExit(handler(args.args))


if __name__ == "__main__":
    main()
