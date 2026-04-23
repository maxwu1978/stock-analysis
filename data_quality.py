"""Quick data completeness checks for CN/US pipelines."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from fetch_data import STOCKS, fetch_all_history
from fetch_us import US_STOCKS, fetch_us_all_history


ROOT = Path(__file__).parent


def summarize_dataset(label: str, expected: dict[str, str], hist: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for code, name in expected.items():
        df = hist.get(code)
        rows.append(
            {
                "market": label,
                "code": code,
                "name": name,
                "available": df is not None and not df.empty,
                "rows": 0 if df is None else len(df),
                "start": "" if df is None or df.empty else str(df.index.min().date()),
                "end": "" if df is None or df.empty else str(df.index.max().date()),
            }
        )
    return pd.DataFrame(rows)


def run() -> None:
    cn_hist = fetch_all_history(days=240)
    us_hist = fetch_us_all_history(period="1y")
    report = pd.concat(
        [
            summarize_dataset("CN", STOCKS, cn_hist),
            summarize_dataset("US", US_STOCKS, us_hist),
        ],
        ignore_index=True,
    )
    out = ROOT / "data_quality_report.csv"
    report.to_csv(out, index=False)

    print("\n══ Data Quality ══")
    print(report.to_string(index=False))
    print(f"\n写出: {out.name}")
    missing = report[~report["available"]]
    if not missing.empty:
        raise SystemExit(1)


if __name__ == "__main__":
    run()
