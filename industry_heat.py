"""行业热度 + 潜力分析

目标:
1. 用问财近似提取当日行业热度
2. 用行业代表股当前 39 因子评分评估实时潜力
3. 叠加既有行业研究先验, 给出更稳妥的候选行业

说明:
- 当前问财没有直接提供稳定的“行业指数级热度”接口, 这里用
  “今日行业板块涨幅排名”返回的活跃个股横截面来近似板块热度.
- “潜力”不是简单追涨, 会同时看实时模型分数和既有行业研究结论.
"""

from __future__ import annotations

import argparse
import re
import time
from dataclasses import dataclass

import numpy as np
import pandas as pd

from fetch_data import fetch_history
from fetch_us import fetch_us_history
from fetch_wencai import get_stock_pool, query
from indicators import compute_all
from probability import score_trend
from probability_us import score_trend_us


TOP_HEAT_INDUSTRIES = 8
MAX_REP_STOCKS = 5
HISTORY_DAYS = 320


INDUSTRY_PROMPT_OVERRIDES = {
    "白酒": "白酒行业股票",
    "房地产": "房地产行业股票",
    "半导体": "半导体芯片股票",
    "创新药": "创新药概念股票",
    "新能源电池": "动力电池概念股",
    "银行": "银行股票",
}


INDUSTRY_RESEARCH_PRIOR = {
    "房地产": {
        "research_score": 88,
        "fit": "反转强",
        "note": "历史研究里是反转适配度最高的行业之一, 39因子在该行业更容易拉开差距",
    },
    "白酒": {
        "research_score": 38,
        "fit": "趋势股",
        "note": "历史研究显示白酒更像趋势行业, 当前A股主反转模型在该行业方向容易失真",
    },
    "半导体": {
        "research_score": 56,
        "fit": "高beta",
        "note": "交易弹性强, 但既有研究显示高收益更多来自行业beta, 不是模型排序能力本身",
    },
    "银行": {
        "research_score": 60,
        "fit": "结构稳",
        "note": "分形结构较稳定, 但行业专属回测还不够",
    },
    "新能源电池": {
        "research_score": 58,
        "fit": "弹性中等",
        "note": "分形结构为正, 但没有像房地产那样明确的行业回测优势",
    },
    "创新药": {
        "research_score": 57,
        "fit": "题材弹性",
        "note": "结构特征不弱, 更适合与热度和实时信号联合判断",
    },
    "生物制品": {
        "research_score": 55,
        "fit": "医药扩散",
        "note": "当前热度高, 但仓库里没有专门行业回测, 先按中性研究先验处理",
    },
    "通信设备": {
        "research_score": 54,
        "fit": "主题扩散",
        "note": "更容易受题材扩散驱动, 需要实时代表股信号确认",
    },
}

AI_FOCUS_PROFILES = {
    "算力服务器": {
        "leaders": ["000977.SZ", "601138.SH", "000063.SZ"],
        "fit": "AI基建",
        "research_score": 62,
        "note": "更受算力投资周期驱动, 适合看龙头是否同步转强",
    },
    "光模块CPO": {
        "leaders": ["300308.SZ", "300502.SZ", "300394.SZ"],
        "fit": "高景气弹性",
        "research_score": 68,
        "note": "AI链里弹性最强的一段之一, 更适合顺势观察而不是逆势抄底",
    },
    "半导体算力芯片": {
        "leaders": ["002371.SZ", "688041.SH", "688256.SH"],
        "fit": "核心算力",
        "research_score": 66,
        "note": "产业位置重要, 但短线容易受高波动和主题切换影响",
    },
    "机器人自动化": {
        "leaders": ["300124.SZ", "002747.SZ", "300024.SZ"],
        "fit": "制造升级",
        "research_score": 64,
        "note": "更偏制造升级主线, 适合看是否出现板块内部扩散",
    },
    "工业母机": {
        "leaders": ["300161.SZ", "601882.SH", "688305.SH"],
        "fit": "设备国产化",
        "research_score": 61,
        "note": "制造业底层设备链, 节奏通常慢于AI主题, 但持续性更重要",
    },
}

US_AI_FOCUS_PROFILES = {
    "算力芯片": {
        "leaders": ["NVDA", "AMD", "AVGO"],
        "fit": "核心算力",
        "research_score": 66,
        "note": "AI主线最核心环节，但当前事件窗口下波动和分化都偏大",
    },
    "晶圆代工": {
        "leaders": ["TSM"],
        "fit": "制造中枢",
        "research_score": 58,
        "note": "产业地位强，但当前模型对TSM的短线可预测性一般",
    },
    "存储": {
        "leaders": ["MU", "WDC"],
        "fit": "景气弹性",
        "research_score": 72,
        "note": "在新纳入的观察链里，存储方向的模型可靠度相对更好",
    },
    "光模块网络": {
        "leaders": ["ANET", "MRVL", "CSCO"],
        "fit": "互联扩容",
        "research_score": 67,
        "note": "更受AI集群扩容和资本开支驱动，适合观察财报后的确认",
    },
    "平台软件": {
        "leaders": ["MSFT", "GOOGL", "AMZN"],
        "fit": "平台承载",
        "research_score": 60,
        "note": "财报周影响大，短线更像事件博弈而不是干净趋势",
    },
}


@dataclass
class LiveIndustrySnapshot:
    avg_score: float
    bullish_pct: float
    recent_20d_ret: float
    sample_n: int
    leaders: str


@dataclass
class UsFocusSnapshot:
    avg_score: float
    bullish_pct: float
    recent_20d_ret: float
    sample_n: int
    leaders: str
    macro_penalty: float


def _extract_rank_num(val: str) -> float:
    m = re.search(r"(\d+)", str(val))
    return float(m.group(1)) if m else np.nan


def _zscore(series: pd.Series) -> pd.Series:
    std = series.std(ddof=0)
    if pd.isna(std) or std == 0:
        return pd.Series(0.0, index=series.index)
    return (series - series.mean()) / std


def fetch_industry_heat() -> pd.DataFrame:
    df = query("今日行业板块涨幅排名")
    if df.empty:
        raise RuntimeError("问财未返回行业热度数据")

    rank_col = next(c for c in df.columns if "行业排名" in c)
    ret_col = next(c for c in df.columns if "涨跌幅:前复权[" in c)

    sub = df[["行业简称", rank_col, ret_col, "股票简称", "股票代码"]].copy()
    sub["rank_num"] = sub[rank_col].map(_extract_rank_num)
    sub[ret_col] = pd.to_numeric(sub[ret_col], errors="coerce")

    grouped = (
        sub.groupby("行业简称")
        .agg(
            best_rank=("rank_num", "min"),
            breadth=("股票代码", "count"),
            avg_ret=(ret_col, "mean"),
        )
        .sort_values(["best_rank", "avg_ret", "breadth"], ascending=[True, False, False])
    )

    sorted_sub = sub.sort_values(["行业简称", "rank_num"])
    leaders = sorted_sub.groupby("行业简称")["股票简称"].apply(
        lambda s: "/".join(s.head(3).astype(str))
    )
    leader_codes = sorted_sub.groupby("行业简称")["股票代码"].apply(
        lambda s: "/".join(s.head(3).astype(str))
    )
    grouped["leaders"] = leaders
    grouped["leader_codes"] = leader_codes
    return grouped.reset_index().rename(columns={"行业简称": "industry"})


def _score_one_stock(code: str) -> tuple[float, float] | None:
    code = str(code).split(".")[0]
    try:
        df = fetch_history(code, days=HISTORY_DAYS)
    except Exception:
        return None
    if df is None or len(df) < 180:
        return None
    try:
        feat = compute_all(df, None)
        res = score_trend(feat)
    except Exception:
        return None
    if "error" in res:
        return None
    ret20 = np.nan
    if len(df) >= 21:
        ret20 = (df["close"].iloc[-1] / df["close"].iloc[-21] - 1) * 100
    return float(res["score"]), float(ret20) if pd.notna(ret20) else np.nan


def fetch_live_snapshot(codes: list[str]) -> LiveIndustrySnapshot | None:
    if not codes:
        return None

    scores = []
    recent_rets = []
    used_codes = []
    for code in codes[:MAX_REP_STOCKS]:
        scored = _score_one_stock(code)
        if scored is None:
            continue
        score, ret20 = scored
        scores.append(score)
        recent_rets.append(ret20)
        used_codes.append(code)
        time.sleep(0.1)

    if not scores:
        return None

    bullish_pct = sum(s >= 8 for s in scores) / len(scores) * 100
    leaders = "/".join(used_codes[:3])
    return LiveIndustrySnapshot(
        avg_score=float(np.mean(scores)),
        bullish_pct=float(bullish_pct),
        recent_20d_ret=float(np.nanmean(recent_rets)),
        sample_n=len(scores),
        leaders=leaders,
    )


def _score_one_us_stock(ticker: str) -> tuple[float, float, float] | None:
    try:
        df = fetch_us_history(ticker, period="5y")
    except Exception:
        return None
    if df is None or len(df) < 180:
        return None
    try:
        feat = compute_all(df, None)
        res = score_trend_us(feat, symbol=ticker)
    except Exception:
        return None
    if "error" in res:
        return None
    ret20 = np.nan
    if len(df) >= 21:
        ret20 = (df["close"].iloc[-1] / df["close"].iloc[-21] - 1) * 100
    penalty = float(res.get("macro_overlay", {}).get("penalty", 0) or 0)
    return float(res["score"]), float(ret20) if pd.notna(ret20) else np.nan, penalty


def fetch_us_focus_snapshot(tickers: list[str]) -> UsFocusSnapshot | None:
    if not tickers:
        return None
    scores = []
    recent_rets = []
    penalties = []
    used = []
    for ticker in tickers[:MAX_REP_STOCKS]:
        scored = _score_one_us_stock(ticker)
        if scored is None:
            continue
        score, ret20, penalty = scored
        scores.append(score)
        recent_rets.append(ret20)
        penalties.append(penalty)
        used.append(ticker)
    if not scores:
        return None
    bullish_pct = sum(s >= 8 for s in scores) / len(scores) * 100
    return UsFocusSnapshot(
        avg_score=float(np.mean(scores)),
        bullish_pct=float(bullish_pct),
        recent_20d_ret=float(np.nanmean(recent_rets)),
        sample_n=len(scores),
        leaders="/".join(used),
        macro_penalty=float(np.mean(penalties)),
    )


def build_report(top_n: int = TOP_HEAT_INDUSTRIES) -> pd.DataFrame:
    heat = fetch_industry_heat().head(top_n).copy()
    rows = []

    for _, row in heat.iterrows():
        industry = row["industry"]
        leader_codes = [c for c in str(row.get("leader_codes", "")).split("/") if c]
        if not leader_codes:
            try:
                leader_codes = get_stock_pool(
                    INDUSTRY_PROMPT_OVERRIDES.get(industry, f"{industry}行业股票"),
                    max_n=MAX_REP_STOCKS,
                )
            except Exception:
                leader_codes = []
        snap = fetch_live_snapshot(leader_codes)
        prior = INDUSTRY_RESEARCH_PRIOR.get(
            industry,
            {"research_score": 50, "fit": "中性", "note": "仓库里暂无该行业专门回测, 先按中性处理"},
        )

        rows.append(
            {
                "industry": industry,
                "best_rank": row["best_rank"],
                "breadth": row["breadth"],
                "avg_ret": row["avg_ret"],
                "heat_leaders": row["leaders"],
                "avg_score": snap.avg_score if snap else np.nan,
                "bullish_pct": snap.bullish_pct if snap else np.nan,
                "recent_20d_ret": snap.recent_20d_ret if snap else np.nan,
                "sample_n": snap.sample_n if snap else 0,
                "live_leaders": snap.leaders if snap else "",
                "research_score": prior["research_score"],
                "fit": prior["fit"],
                "note": prior["note"],
            }
        )

    report = pd.DataFrame(rows)
    if report.empty:
        return report

    heat_score = (
        0.45 * (100 - (report["best_rank"] - 1) / max(report["best_rank"].max() - 1, 1) * 100)
        + 0.30 * (_zscore(report["avg_ret"]).clip(-2, 2) + 2) / 4 * 100
        + 0.25 * (_zscore(report["breadth"]).clip(-2, 2) + 2) / 4 * 100
    )

    live_score = (
        0.55 * ((report["avg_score"].fillna(0).clip(-100, 100) + 100) / 2)
        + 0.30 * report["bullish_pct"].fillna(0)
        + 0.15 * ((_zscore(report["recent_20d_ret"].fillna(0)).clip(-2, 2) + 2) / 4 * 100)
    )

    report["heat_score"] = heat_score.round(1)
    report["live_score"] = live_score.round(1)
    report["potential_score"] = (
        0.35 * report["heat_score"]
        + 0.40 * report["live_score"]
        + 0.25 * report["research_score"]
    ).round(1)

    report["signal_view"] = np.where(
        report["avg_score"].fillna(0) >= 25,
        "偏多",
        np.where(
            report["avg_score"].fillna(0) >= 8,
            "观察偏多",
            np.where(
                report["avg_score"].fillna(0) <= -25,
                "偏弱",
                np.where(report["avg_score"].fillna(0) <= -8, "转弱", "中性"),
            ),
        ),
    )

    report = report.sort_values(["potential_score", "heat_score"], ascending=False).reset_index(drop=True)
    return report


def print_report(report: pd.DataFrame) -> None:
    if report.empty:
        print("无有效行业数据.")
        return

    print("\n" + "=" * 92)
    print("  行业热度 + 潜力分析")
    print("=" * 92)

    hot_cols = ["industry", "best_rank", "breadth", "avg_ret", "heat_score", "heat_leaders"]
    hot = report.sort_values(["heat_score", "potential_score"], ascending=False)[hot_cols].head(8).copy()
    hot = hot.rename(
        columns={
            "industry": "行业",
            "best_rank": "热度位次",
            "breadth": "入榜股数",
            "avg_ret": "样本均涨幅%",
            "heat_score": "热度分",
            "heat_leaders": "热度龙头",
        }
    )
    print("\n[1] 今日热度靠前行业")
    print(hot.to_string(index=False, float_format=lambda x: f"{x:.1f}"))

    pot_cols = [
        "industry",
        "potential_score",
        "signal_view",
        "avg_score",
        "bullish_pct",
        "research_score",
        "fit",
        "note",
    ]
    pot = report[pot_cols].head(6).copy()
    pot = pot.rename(
        columns={
            "industry": "行业",
            "potential_score": "潜力分",
            "signal_view": "当前视图",
            "avg_score": "代表股均分",
            "bullish_pct": "偏多占比%",
            "research_score": "研究先验",
            "fit": "行业画像",
            "note": "判断依据",
        }
    )
    print("\n[2] 当前更有潜力的行业")
    print(pot.to_string(index=False, float_format=lambda x: f"{x:.1f}"))

    print("\n[3] 解读")
    for _, row in report.head(3).iterrows():
        print(
            f"  - {row['industry']}: 热度分 {row['heat_score']:.1f}, 实时分 {row['live_score']:.1f}, "
            f"研究先验 {row['research_score']:.0f}. {row['note']}"
        )

    weak = report.nsmallest(2, "potential_score")
    print("\n[4] 暂不优先追踪")
    for _, row in weak.iterrows():
        print(
            f"  - {row['industry']}: 当前视图={row['signal_view']}，代表股均分 {row['avg_score']:.1f}，"
            f"热度虽在榜但综合潜力偏弱。"
        )


def build_focus_report(profiles: dict[str, dict]) -> pd.DataFrame:
    rows = []
    for industry, cfg in profiles.items():
        snap = fetch_live_snapshot(cfg["leaders"])
        if snap is None:
            continue
        rows.append(
            {
                "industry": industry,
                "avg_score": snap.avg_score,
                "bullish_pct": snap.bullish_pct,
                "recent_20d_ret": snap.recent_20d_ret,
                "sample_n": snap.sample_n,
                "leaders": snap.leaders,
                "research_score": cfg["research_score"],
                "fit": cfg["fit"],
                "note": cfg["note"],
            }
        )

    report = pd.DataFrame(rows)
    if report.empty:
        return report

    live_score = (
        0.65 * ((report["avg_score"].clip(-100, 100) + 100) / 2)
        + 0.20 * report["bullish_pct"]
        + 0.15 * ((_zscore(report["recent_20d_ret"]).clip(-2, 2) + 2) / 4 * 100)
    )
    report["live_score"] = live_score.round(1)
    report["potential_score"] = (0.60 * report["live_score"] + 0.40 * report["research_score"]).round(1)
    report["signal_view"] = np.where(
        report["avg_score"] >= 25,
        "偏多",
        np.where(
            report["avg_score"] >= 8,
            "观察偏多",
            np.where(
                report["avg_score"] <= -25,
                "偏弱",
                np.where(report["avg_score"] <= -8, "转弱", "中性"),
            ),
        ),
    )
    return report.sort_values("potential_score", ascending=False).reset_index(drop=True)


def print_focus_report(report: pd.DataFrame) -> None:
    if report.empty:
        print("无有效AI焦点行业数据.")
        return

    print("\n" + "=" * 92)
    print("  AI科技/制造 焦点行业分析")
    print("=" * 92)
    table = report[
        [
            "industry",
            "potential_score",
            "signal_view",
            "avg_score",
            "bullish_pct",
            "recent_20d_ret",
            "fit",
            "note",
        ]
    ].copy()
    table = table.rename(
        columns={
            "industry": "行业",
            "potential_score": "潜力分",
            "signal_view": "当前视图",
            "avg_score": "代表股均分",
            "bullish_pct": "偏多占比%",
            "recent_20d_ret": "近20日涨幅%",
            "fit": "行业画像",
            "note": "判断依据",
        }
    )
    print(table.to_string(index=False, float_format=lambda x: f"{x:.1f}"))

    print("\n[结论]")
    top = report.iloc[0]
    print(
        f"  - 当前最值得优先盯的是 {top['industry']}，代表股均分 {top['avg_score']:.1f}，"
        f"偏多占比 {top['bullish_pct']:.1f}% 。"
    )
    for _, row in report.tail(2).iterrows():
        print(
            f"  - {row['industry']} 当前更像观察而不是追击，视图={row['signal_view']}，"
            f"代表股均分 {row['avg_score']:.1f}。"
        )


def build_us_focus_report(profiles: dict[str, dict]) -> pd.DataFrame:
    rows = []
    for industry, cfg in profiles.items():
        snap = fetch_us_focus_snapshot(cfg["leaders"])
        if snap is None:
            continue
        rows.append(
            {
                "industry": industry,
                "avg_score": snap.avg_score,
                "bullish_pct": snap.bullish_pct,
                "recent_20d_ret": snap.recent_20d_ret,
                "sample_n": snap.sample_n,
                "leaders": snap.leaders,
                "macro_penalty": snap.macro_penalty,
                "research_score": cfg["research_score"],
                "fit": cfg["fit"],
                "note": cfg["note"],
            }
        )
    report = pd.DataFrame(rows)
    if report.empty:
        return report
    live_score = (
        0.60 * ((report["avg_score"].clip(-100, 100) + 100) / 2)
        + 0.20 * report["bullish_pct"]
        + 0.10 * ((_zscore(report["recent_20d_ret"]).clip(-2, 2) + 2) / 4 * 100)
        + 0.10 * (100 - report["macro_penalty"].clip(0, 30) / 30 * 100)
    )
    report["live_score"] = live_score.round(1)
    report["potential_score"] = (0.65 * report["live_score"] + 0.35 * report["research_score"]).round(1)
    report["signal_view"] = np.where(
        report["avg_score"] >= 25,
        "偏多",
        np.where(
            report["avg_score"] >= 8,
            "观察偏多",
            np.where(
                report["avg_score"] <= -25,
                "偏弱",
                np.where(report["avg_score"] <= -8, "转弱", "中性"),
            ),
        ),
    )
    return report.sort_values("potential_score", ascending=False).reset_index(drop=True)


def print_us_focus_report(report: pd.DataFrame) -> None:
    if report.empty:
        print("无有效美股AI焦点数据.")
        return
    print("\n" + "=" * 92)
    print("  美股 AI/制造 焦点行业分析")
    print("=" * 92)
    table = report[
        [
            "industry",
            "potential_score",
            "signal_view",
            "avg_score",
            "bullish_pct",
            "recent_20d_ret",
            "macro_penalty",
            "fit",
            "note",
        ]
    ].copy()
    table = table.rename(
        columns={
            "industry": "方向",
            "potential_score": "潜力分",
            "signal_view": "当前视图",
            "avg_score": "代表股均分",
            "bullish_pct": "偏多占比%",
            "recent_20d_ret": "近20日涨幅%",
            "macro_penalty": "宏观扣分",
            "fit": "画像",
            "note": "判断依据",
        }
    )
    print(table.to_string(index=False, float_format=lambda x: f"{x:.1f}"))

    print("\n[结论]")
    top = report.iloc[0]
    print(
        f"  - 当前美股链里相对更值得优先盯的是 {top['industry']}，代表股均分 {top['avg_score']:.1f}，"
        f"宏观平均扣分 {top['macro_penalty']:.1f}。"
    )
    tail = report.iloc[-1]
    print(
        f"  - 当前最弱的是 {tail['industry']}，视图={tail['signal_view']}，代表股均分 {tail['avg_score']:.1f}。"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--focus", choices=["ai", "us-ai"], default=None)
    args = parser.parse_args()

    if args.focus == "ai":
        report = build_focus_report(AI_FOCUS_PROFILES)
        if report.empty:
            raise SystemExit(1)
        report.to_csv("industry_ai_focus_report.csv", index=False)
        print_focus_report(report)
        print("\n明细保存: industry_ai_focus_report.csv")
        return

    if args.focus == "us-ai":
        report = build_us_focus_report(US_AI_FOCUS_PROFILES)
        if report.empty:
            raise SystemExit(1)
        report.to_csv("industry_us_ai_focus_report.csv", index=False)
        print_us_focus_report(report)
        print("\n明细保存: industry_us_ai_focus_report.csv")
        return

    report = build_report()
    if report.empty:
        raise SystemExit(1)
    report.to_csv("industry_heat_report.csv", index=False)
    print_report(report)
    print("\n明细保存: industry_heat_report.csv")


if __name__ == "__main__":
    main()
