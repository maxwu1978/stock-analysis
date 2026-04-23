"""Build executable watchlists from CN/US model actions."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from fetch_data import STOCKS, fetch_all_history
from fetch_us import US_STOCKS, fetch_us_all_history
from fundamental import fetch_all_financials
from fetch_us import fetch_us_financials
from indicators import compute_all
from probability import score_trend
from probability_us import score_trend_us
from position_sizing import recommend_model_action
from reliability import get_reliability_label, load_reliability_labels


ROOT = Path(__file__).parent


def _derive_direction(hp: dict) -> str:
    p30 = hp.get("30日")
    if not p30:
        return "震荡"
    pct = int(p30["上涨概率"].replace("%", ""))
    if pct >= 60:
        return "看涨"
    if pct >= 55:
        return "偏涨"
    if pct <= 35:
        return "看跌"
    if pct <= 45:
        return "偏跌"
    return "震荡"


def build_cn_watchlist(labels: dict) -> pd.DataFrame:
    hist = fetch_all_history(days=240)
    fund = fetch_all_financials()
    rows = []
    for code, df in hist.items():
        df = compute_all(df, fund.get(code))
        prob = score_trend(df)
        if "error" in prob:
            continue
        direction = _derive_direction(prob.get("historical_prob", {}))
        decision = recommend_model_action(
            direction=direction,
            entry_price=float(df["close"].iloc[-1]),
            score=prob.get("score"),
            reliability=get_reliability_label(labels, "a_share", code),
            macro_penalty=0,
        )
        rows.append(
            {
                "market": "CN",
                "code": code,
                "name": STOCKS[code],
                "direction": direction,
                "reliability": get_reliability_label(labels, "a_share", code),
                "action": decision.action,
                "plan_tier": decision.plan.position_tier,
                "qty": decision.plan.qty,
                "risk_budget": round(decision.plan.risk_budget, 2),
                "score": round(float(prob.get("score", 0) or 0), 2),
                "close": round(float(df["close"].iloc[-1]), 2),
                "rationale": decision.rationale,
            }
        )
    return pd.DataFrame(rows).sort_values(["plan_tier", "score"], ascending=[True, False])


def build_us_watchlist(labels: dict) -> pd.DataFrame:
    hist = fetch_us_all_history(period="1y")
    fund = fetch_us_financials()
    rows = []
    for ticker, df in hist.items():
        df = compute_all(df, fund.get(ticker))
        prob = score_trend_us(df, symbol=ticker)
        if "error" in prob:
            continue
        direction = _derive_direction(prob.get("historical_prob", {}))
        penalty = int(prob.get("macro_overlay", {}).get("penalty", 0) or 0)
        decision = recommend_model_action(
            direction=direction,
            entry_price=float(df["close"].iloc[-1]),
            score=prob.get("score"),
            reliability=get_reliability_label(labels, "us", ticker),
            macro_penalty=penalty,
        )
        rows.append(
            {
                "market": "US",
                "code": ticker,
                "name": US_STOCKS[ticker],
                "direction": direction,
                "reliability": get_reliability_label(labels, "us", ticker),
                "action": decision.action,
                "plan_tier": decision.plan.position_tier,
                "qty": decision.plan.qty,
                "risk_budget": round(decision.plan.risk_budget, 2),
                "macro_penalty": penalty,
                "score": round(float(prob.get("score", 0) or 0), 2),
                "close": round(float(df["close"].iloc[-1]), 2),
                "rationale": decision.rationale,
            }
        )
    return pd.DataFrame(rows).sort_values(["plan_tier", "score"], ascending=[True, False])


def run() -> None:
    labels = load_reliability_labels()
    now = datetime.now().strftime("%Y%m%d_%H%M")
    cn = build_cn_watchlist(labels)
    us = build_us_watchlist(labels)

    cn_path = ROOT / "execution_watchlist_cn.csv"
    us_path = ROOT / "execution_watchlist_us.csv"
    snapshot_path = ROOT / f"execution_watchlist_{now}.csv"

    if not cn.empty:
        cn.to_csv(cn_path, index=False)
    if not us.empty:
        us.to_csv(us_path, index=False)
    pd.concat([cn, us], ignore_index=True).to_csv(snapshot_path, index=False)

    print("\n══ Execution Watchlist ══")
    if not cn.empty:
        print("\n[CN]")
        print(cn[["name", "action", "plan_tier", "qty", "risk_budget", "score"]].to_string(index=False))
        print(f"\n写出: {cn_path.name}")
    if not us.empty:
        print("\n[US]")
        print(us[["name", "action", "plan_tier", "qty", "risk_budget", "score", "macro_penalty"]].to_string(index=False))
        print(f"\n写出: {us_path.name}")
    print(f"\n快照: {snapshot_path.name}")


if __name__ == "__main__":
    run()
