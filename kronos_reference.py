#!/usr/bin/env python3
"""Helpers for loading/surfacing Kronos sidecar reference snapshots."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


SNAPSHOT_PATH = Path(__file__).with_name("kronos_reference_snapshot.csv")


def load_kronos_reference(path: Path = SNAPSHOT_PATH) -> dict[tuple[str, str], dict]:
    if not path.exists():
        return {}
    try:
        df = pd.read_csv(path)
    except Exception:
        return {}

    refs: dict[tuple[str, str], dict] = {}
    for _, row in df.iterrows():
        market = str(row.get("market", "") or "").upper()
        symbol = str(row.get("symbol", "") or "")
        if not market or not symbol:
            continue
        refs[(market, symbol)] = row.to_dict()
    return refs


def get_kronos_reference(refs: dict[tuple[str, str], dict], market: str, symbol: str) -> dict | None:
    return refs.get((market.upper(), symbol))


def format_kronos_reference_text(ref: dict | None) -> str:
    if not ref:
        return "-"
    direction = ref.get("direction", "-")
    pred_ret = ref.get("pred_ret_5d_pct")
    model = ref.get("model", "mini")
    if pred_ret is None or pd.isna(pred_ret):
        return f"{direction}"
    val = float(pred_ret)
    if abs(val) < 0.005:
        val = 0.0
    sign = "+" if val > 0 else ""
    return f"{direction} {sign}{val:.2f}% ({model})"


def format_kronos_reference_html(ref: dict | None) -> str:
    if not ref:
        return (
            '<td class="kronos-cell" data-label="Kronos参考">'
            '<div class="kronos-stack"><span class="kronos-chip">-</span>'
            '<span class="kronos-meta">未覆盖</span></div></td>'
        )
    direction = str(ref.get("direction", "-"))
    pred_ret = ref.get("pred_ret_5d_pct")
    model = str(ref.get("model", "mini"))
    chip_cls = "kronos-chip"
    if "看涨" in direction:
        chip_cls += " kronos-up"
    elif "看跌" in direction:
        chip_cls += " kronos-down"
    if pred_ret is None or pd.isna(pred_ret):
        ret_text = "-"
    else:
        val = float(pred_ret)
        if abs(val) < 0.005:
            val = 0.0
        ret_text = f"{'+' if val > 0 else ''}{val:.2f}%"
    return (
        '<td class="kronos-cell" data-label="Kronos参考">'
        '<div class="kronos-stack">'
        f'<span class="{chip_cls}">{direction}</span>'
        f'<span class="kronos-meta">{ret_text} · {model} · 仅研究</span>'
        '</div></td>'
    )
