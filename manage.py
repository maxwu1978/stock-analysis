#!/usr/bin/env python3
"""Unified local command runner for the stock-analysis project.

This wrapper intentionally keeps the existing scripts in place. It only
standardizes the commands used during daily operation and release checks.
"""

from __future__ import annotations

import argparse
import os
import shutil
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
    parser = argparse.ArgumentParser(prog="manage.py refresh-page", add_help=False)
    parser.add_argument("--strict", action="store_true", help="run generate_page.py without --allow-partial")
    known, passthrough = parser.parse_known_args(extra)
    if known.strict:
        args = passthrough
    else:
        args = passthrough or ["--allow-partial"]
    return python_cmd("generate_page.py", args)


def cmd_refresh_us_quotes(extra: list[str]) -> int:
    return python_cmd("refresh_us_quotes_page.py", extra)


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


def cmd_factor_ideas(extra: list[str]) -> int:
    return python_cmd("factor_idea_generator.py", extra)


def cmd_factor_lab(extra: list[str]) -> int:
    return python_cmd("factor_lab.py", extra)


def cmd_factor_promotion(extra: list[str]) -> int:
    return python_cmd("factor_promotion.py", extra)


def cmd_validate_a(extra: list[str]) -> int:
    return python_cmd("a_share_signal_validation_2y.py", extra)


def cmd_capital_flow_backtest(extra: list[str]) -> int:
    return python_cmd("cn_capital_flow_backtest.py", extra)


def cmd_industry_heat(extra: list[str]) -> int:
    return python_cmd("industry_heat.py", extra)


def cmd_kronos_reference(extra: list[str]) -> int:
    return python_cmd("build_kronos_reference.py", extra)


def cmd_kronos_us_experiment(extra: list[str]) -> int:
    return python_cmd("kronos_us_experiment.py", extra)


def cmd_kronos_confirm_us(extra: list[str]) -> int:
    return python_cmd("kronos_confirmation_backtest.py", extra)


def cmd_kronos_confirm_a(extra: list[str]) -> int:
    return python_cmd("kronos_confirmation_backtest_a.py", extra)


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


def cmd_doctor(extra: list[str]) -> int:
    if extra:
        raise SystemExit("doctor does not accept extra arguments")

    failures = 0

    def check(ok: bool, message: str) -> None:
        nonlocal failures
        status = "OK" if ok else "FAIL"
        print(f"[{status}] {message}")
        if not ok:
            failures += 1

    required_files = [
        "README.md",
        "COMMANDS.md",
        ".github/workflows/factor-lab.yml",
        ".github/workflows/update-page.yml",
        "requirements.txt",
        "docs/index.html",
        "manage.py",
        "install_launchd.sh",
    ]
    for name in required_files:
        check((ROOT / name).exists(), f"required file exists: {name}")

    try:
        import yaml  # type: ignore[import-not-found]
    except ImportError:
        print("[SKIP] PyYAML is not available; workflow syntax was not checked")
    else:
        for workflow in sorted((ROOT / ".github/workflows").glob("*.yml")):
            try:
                yaml.safe_load(workflow.read_text(encoding="utf-8"))
                check(True, f"workflow yaml is parseable: {workflow.name}")
            except Exception as exc:
                check(False, f"workflow yaml is parseable: {workflow.name}")
                print(f"  {exc}")

    shell_scripts = [
        "auto_hedge_daily.sh",
        "install_launchd.sh",
        "run_advisor_daily.sh",
        "run_factor_learning_daily.sh",
        "run_factor_testing_daily.sh",
        "run_option_monitor.sh",
        "scripts/factor_learn.sh",
        "scripts/factor_test.sh",
        "scripts/preflight.sh",
        "scripts/refresh_option_strategy.sh",
        "scripts/run_factor_lab_ci.sh",
        "scripts/scan_a_opportunities.sh",
    ]
    for name in shell_scripts:
        path = ROOT / name
        check(path.exists(), f"script exists: {name}")
        if path.exists():
            check(os.access(path, os.X_OK), f"script is executable: {name}")

    legacy_path = "/Volumes/MaxRelocated/主力分析"
    portable_scripts = ["auto_hedge_daily.sh", "run_advisor_daily.sh", "run_option_monitor.sh"]
    for name in portable_scripts:
        path = ROOT / name
        if path.exists():
            check(legacy_path not in path.read_text(encoding="utf-8"), f"no legacy hard-coded project path: {name}")

    plists = sorted(ROOT.glob("com.maxwu.*.plist"))
    check(bool(plists), "launchd plist templates exist")
    if shutil.which("plutil"):
        for plist in plists:
            completed = subprocess.run(["plutil", "-lint", str(plist)], cwd=ROOT, text=True, capture_output=True)
            check(completed.returncode == 0, f"plist is valid: {plist.name}")
            if completed.returncode != 0:
                print(completed.stdout + completed.stderr, end="")
    else:
        print("[SKIP] plutil is not available; plist syntax was not checked")

    if failures:
        print(f"doctor found {failures} issue(s)")
        return 1
    print("doctor checks passed")
    return 0


def cmd_preflight(extra: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="manage.py preflight")
    parser.add_argument("--remote", action="store_true", help="also smoke-test GitHub Pages")
    args = parser.parse_args(extra)

    doctor_status = cmd_doctor([])
    if doctor_status != 0:
        return doctor_status
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
    "doctor": (cmd_doctor, "Check local automation wiring and entrypoint files."),
    "factor-ideas": (cmd_factor_ideas, "Generate draft factor candidate ideas."),
    "factor-lab": (cmd_factor_lab, "Batch-evaluate candidate factors."),
    "factor-learn": (cmd_factor_learn, "Run candidate factor learning."),
    "factor-promotion": (cmd_factor_promotion, "Build factor promotion queue from lab outputs."),
    "factor-test": (cmd_factor_test, "Run candidate factor testing."),
    "fetch-option-chains": (cmd_fetch_option_chains, "Fetch needed historical option-chain slices."),
    "import-option-chains": (cmd_import_option_chains, "Import third-party option-chain CSV snapshots."),
    "industry-heat": (cmd_industry_heat, "Run industry heat and potential analysis."),
    "kronos-confirm-a": (cmd_kronos_confirm_a, "Run A-share Kronos confirmation backtest."),
    "kronos-confirm-us": (cmd_kronos_confirm_us, "Run U.S. Kronos confirmation backtest."),
    "kronos-reference": (cmd_kronos_reference, "Build research-only Kronos reference snapshot."),
    "kronos-us-experiment": (cmd_kronos_us_experiment, "Run minimal U.S. Kronos experiment."),
    "list-commands": (cmd_list_commands, "Print available project commands."),
    "option-account-sim": (cmd_option_account_sim, "Simulate account equity from option trades."),
    "option-pnl-review": (cmd_option_pnl_review, "Review historical option proxy PnL."),
    "option-signal-review": (cmd_option_signal_review, "Replay historical strong/weak option signals."),
    "refresh-page": (cmd_refresh_page, "Generate docs pages. Defaults to --allow-partial; use --strict for release mode."),
    "refresh-options": (cmd_refresh_options, "Refresh option strategy output."),
    "refresh-us-quotes": (cmd_refresh_us_quotes, "Refresh U.S. quote snapshot pages."),
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
