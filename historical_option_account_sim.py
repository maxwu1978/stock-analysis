"""Account-level simulation for historical option signal proxy trades."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from position_sizing import recommend_long_option_position, recommend_straddle_position


ROOT = Path(__file__).parent


def _page_reference_qty(row: pd.Series) -> int:
    if row.kind == "single":
        return recommend_long_option_position(
            premium=float(row.entry_debit),
            account_equity=1_000_000,
            reliability="中",
            confidence="HIGH",
        ).qty
    return recommend_straddle_position(
        total_premium=float(row.entry_debit),
        account_equity=1_000_000,
        confidence="MEDIUM",
    ).qty


def _strict_budget_qty(row: pd.Series, equity: float) -> int:
    if row.kind == "single":
        risk_budget = equity * 0.0030 * 0.60
        risk_per_contract = float(row.entry_debit_per_contract) * 0.60
        return min(3, int(risk_budget // risk_per_contract)) if risk_per_contract > 0 else 0
    risk_budget = equity * 0.0020 * 0.75
    risk_per_contract = float(row.entry_debit_per_contract) * 0.65
    return min(2, int(risk_budget // risk_per_contract)) if risk_per_contract > 0 else 0


def _qty_for_mode(row: pd.Series, equity: float, mode: str) -> int:
    if mode == "min-contract":
        return 1
    if mode == "page-reference":
        return _page_reference_qty(row)
    if mode == "strict-budget":
        return _strict_budget_qty(row, equity)
    raise ValueError(f"unknown mode: {mode}")


def simulate(
    trades: pd.DataFrame,
    *,
    initial_capital: float,
    mode: str,
    friction_rate: float,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    signals = trades[trades["is_episode_entry"]].sort_values(["date", "symbol", "kind"]).copy()
    cash = float(initial_capital)
    open_positions: list[dict] = []
    ledger: list[dict] = []
    skipped: list[dict] = []
    min_cash = cash

    for _, row in signals.iterrows():
        still_open: list[dict] = []
        for pos in open_positions:
            if pos["exit_date"] <= row.date:
                cash += pos["exit_cash"]
                ledger.append({**pos, "event": "exit", "cash_after": cash})
            else:
                still_open.append(pos)
        open_positions = still_open

        marked_equity = cash + sum(max(0.0, pos["exit_cash"]) for pos in open_positions)
        qty = int(_qty_for_mode(row, marked_equity, mode))
        entry_cost = float(row.entry_debit_per_contract) * qty
        entry_friction = entry_cost * friction_rate / 2
        exit_friction = entry_cost * friction_rate / 2
        required_cash = entry_cost + entry_friction

        if qty <= 0 or required_cash > cash:
            skipped.append({
                "date": row.date,
                "symbol": row.symbol,
                "kind": row.kind,
                "reason": "no_qty" if qty <= 0 else "cash",
                "qty": qty,
                "required_cash": required_cash,
                "cash": cash,
            })
            continue

        cash -= required_cash
        pnl_cash = float(row.pnl_per_contract) * qty - entry_friction - exit_friction
        exit_cash = entry_cost + float(row.pnl_per_contract) * qty - exit_friction
        pos = {
            "entry_date": row.date,
            "exit_date": row.exit_date,
            "symbol": row.symbol,
            "kind": row.kind,
            "qty": qty,
            "entry_cost": entry_cost,
            "entry_friction": entry_friction,
            "exit_friction": exit_friction,
            "pnl_cash": pnl_cash,
            "exit_cash": exit_cash,
            "pnl_pct": row.pnl_pct,
            "exit_reason": row.exit_reason,
        }
        open_positions.append(pos)
        ledger.append({**pos, "event": "entry", "cash_after": cash})
        min_cash = min(min_cash, cash)

    for pos in sorted(open_positions, key=lambda x: x["exit_date"]):
        cash += pos["exit_cash"]
        ledger.append({**pos, "event": "exit", "cash_after": cash})

    ledger_df = pd.DataFrame(ledger)
    skipped_df = pd.DataFrame(skipped)
    entries = ledger_df[ledger_df["event"] == "entry"].copy() if not ledger_df.empty else pd.DataFrame()
    summary = {
        "mode": mode,
        "initial_capital": initial_capital,
        "friction_rate": friction_rate,
        "final_equity": cash,
        "return_pct": (cash / initial_capital - 1) * 100 if initial_capital else 0,
        "trades": len(entries),
        "skipped": len(skipped_df),
        "wins": int((entries["pnl_cash"] > 0).sum()) if not entries.empty else 0,
        "win_rate_pct": float((entries["pnl_cash"] > 0).mean() * 100) if not entries.empty else 0.0,
        "gross_pnl": float(entries["pnl_cash"].sum()) if not entries.empty else 0.0,
        "avg_pnl": float(entries["pnl_cash"].mean()) if not entries.empty else 0.0,
        "best_trade": float(entries["pnl_cash"].max()) if not entries.empty else 0.0,
        "worst_trade": float(entries["pnl_cash"].min()) if not entries.empty else 0.0,
        "min_cash": min_cash,
    }
    return ledger_df, skipped_df, summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulate account equity from historical option proxy trades")
    parser.add_argument("--trades", type=Path, default=ROOT / "historical_option_pnl_review_v3.csv")
    parser.add_argument("--initial-capital", type=float, default=10_000)
    parser.add_argument("--mode", choices=["min-contract", "page-reference", "strict-budget"], default="min-contract")
    parser.add_argument("--friction-rate", type=float, default=0.0, help="Round-trip option friction as fraction of entry debit")
    parser.add_argument("--output-prefix", type=Path, default=ROOT / "historical_option_account_sim")
    args = parser.parse_args()

    trades = pd.read_csv(args.trades, parse_dates=["date", "exit_date"])
    ledger, skipped, summary = simulate(
        trades,
        initial_capital=args.initial_capital,
        mode=args.mode,
        friction_rate=args.friction_rate,
    )

    ledger_path = Path(f"{args.output_prefix}_{args.mode}.csv")
    skipped_path = Path(f"{args.output_prefix}_{args.mode}_skipped.csv")
    summary_path = Path(f"{args.output_prefix}_{args.mode}_summary.csv")
    ledger.to_csv(ledger_path, index=False)
    skipped.to_csv(skipped_path, index=False)
    pd.DataFrame([summary]).to_csv(summary_path, index=False)

    print("\n══ Historical Option Account Simulation ══")
    for key, value in summary.items():
        if isinstance(value, float):
            print(f"{key}: {value:,.2f}")
        else:
            print(f"{key}: {value}")
    print(f"ledger: {ledger_path}")
    print(f"summary: {summary_path}")
    print(f"skipped: {skipped_path}")


if __name__ == "__main__":
    main()
