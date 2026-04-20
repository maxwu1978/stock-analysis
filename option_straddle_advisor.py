"""跨式 / 宽跨式策略推荐器 (基于 Δα 与 IV 分形信号)

Straddle = 同行权价 Call + Put (赌方向性爆发, 方向不限)
Strangle = OTM Call + OTM Put (更便宜但需更大波动才盈利)

分形信号映射:
  Δα > 0.6 (波动结构复杂) + IV 历史分位 <40% (低估) → 买 Straddle/Strangle
  Δα 大意味着多尺度波动不均匀, 历史上常伴随大幅度突破前的"积累"
  IV 低意味着市场定价还未反映即将到来的波动 (cheap options)

盈亏平衡点:
  Straddle breakeven = strike ± (call_premium + put_premium)
  Strangle breakeven = call_strike + 总成本 / put_strike - 总成本

运行:
  python option_straddle_advisor.py              # 默认科技股关注池
  python option_straddle_advisor.py nvda_only
"""

import sys
import numpy as np
import pandas as pd
from datetime import datetime

from fetch_futu import realtime_quotes, get_kline, find_atm_options, health_check
from fractal_survey import mfdfa_spectrum
from iv_rank import get_iv_rank_best_effort, describe_rank, log_iv


WATCHLISTS = {
    "tech": ["US.NVDA", "US.AAPL", "US.MSFT", "US.GOOGL", "US.TSLA", "US.META", "US.AMZN"],
    "default": ["US.NVDA", "US.AAPL", "US.TSLA"],
    "nvda_only": ["US.NVDA"],
}


def compute_iv_rank(historical_iv: pd.Series, current_iv: float) -> float:
    """IV 百分位 (0-100). 当前 IV 在过去 252 天排名."""
    if len(historical_iv) < 60 or pd.isna(current_iv):
        return np.nan
    hist_clean = historical_iv.dropna().tail(252)
    if len(hist_clean) < 10:
        return np.nan
    return (hist_clean < current_iv).mean() * 100


def analyze_stock(code: str) -> dict:
    """计算分形 + 实际波动率 + IV."""
    out = {"code": code}

    rt = realtime_quotes([code])
    out["spot"] = float(rt.iloc[0]["last_price"])

    # 历史
    kl = get_kline(code, days=400, ktype="K_DAY")
    closes = kl["close"].astype(float)
    out["_closes"] = closes  # 保留给 IV rank 计算
    log_ret = np.log(closes / closes.shift(1))

    # MF-DFA
    if len(log_ret) >= 120:
        spec = mfdfa_spectrum(log_ret.iloc[-120:])
        out.update(spec)

    # 实际波动率 (20日, 年化)
    out["realized_vol"] = log_ret.iloc[-20:].std() * np.sqrt(252) * 100

    return out


def score_straddle_opportunity(feat: dict, atm_chain: pd.DataFrame) -> dict:
    """给 straddle 机会打分.

    强信号条件:
      - Δα > 0.6 (分形复杂)
      - IV < realized vol 历史 (低估, 期权便宜)
      - IV 百分位 < 40% (相对便宜, 需要较长IV历史, 当前用 realized vol 代理)

    返回: signal (BUY_STRADDLE / WAIT / OBSERVE) + 盈亏平衡点
    """
    delta_a = feat.get("delta_alpha", 0) or 0
    realized_vol = feat.get("realized_vol", 0)

    # 取 ATM 的 Call 和 Put (最接近现价)
    calls = atm_chain[atm_chain["option_type"] == "CALL"].copy()
    puts = atm_chain[atm_chain["option_type"] == "PUT"].copy()
    if calls.empty or puts.empty:
        return {"signal": "NO_CHAIN"}

    calls["abs_money"] = calls["moneyness_pct"].abs()
    puts["abs_money"] = puts["moneyness_pct"].abs()
    atm_call = calls.nsmallest(1, "abs_money").iloc[0]
    atm_put = puts.nsmallest(1, "abs_money").iloc[0]

    straddle_cost = atm_call["last_price"] + atm_put["last_price"]
    spot = feat["spot"]
    # 盈亏平衡 (±cost)
    breakeven_upper = atm_call["strike_price"] + straddle_cost
    breakeven_lower = atm_put["strike_price"] - straddle_cost
    # 需要的价格变动幅度 (%)
    move_needed_pct = straddle_cost / spot * 100

    # 隐含波动率 (两腿均值)
    iv_avg = (atm_call["iv"] + atm_put["iv"]) / 2

    # 记录 IV 到本地历史 (每次跑都追加, 累积到 252 天后切换为真实 IV Rank)
    closes = feat.get("_closes")
    if closes is not None and iv_avg > 0:
        log_iv(atm_call["code"], feat["code"], iv_avg,
               atm_call.get("expiry", ""), atm_call["strike_price"])

    # IV Rank: 先尝试真实历史, 再回落到 realized vol 代理
    iv_rank, rank_source = get_iv_rank_best_effort(
        feat["code"], iv_avg, closes, realized_vol
    )
    iv_cheap = iv_avg < realized_vol  # 保留原判断作兼容

    # 决策
    result = {
        "atm_strike": atm_call["strike_price"],
        "call_code": atm_call["code"],
        "put_code": atm_put["code"],
        "call_premium": atm_call["last_price"],
        "put_premium": atm_put["last_price"],
        "straddle_cost": straddle_cost,
        "breakeven_upper": breakeven_upper,
        "breakeven_lower": breakeven_lower,
        "move_needed_pct": move_needed_pct,
        "iv_avg": iv_avg,
        "realized_vol": realized_vol,
        "iv_cheap_vs_realized": iv_cheap,
        "iv_rank": iv_rank,
        "iv_rank_source": rank_source,
        "iv_rank_desc": describe_rank(iv_rank, rank_source),
        "delta_alpha": delta_a,
        "days_to_expiry": int(atm_call.get("days_to_expiry", 0)),
    }

    # 决策: 有 IV Rank 时用 IV Rank (更权威), 否则回落 realized vol 代理
    if iv_rank is not None:
        iv_low = iv_rank < 30
        iv_high = iv_rank > 70
    else:
        iv_low = iv_cheap
        iv_high = False  # 代理无法判断高位, 保守

    # 按优先级判定 (IV_HIGH 先拦截, 再考虑分形+IV 便宜的买入机会)
    if delta_a > 0.6 and iv_high:
        # 分形复杂但 IV 贵 → 买跨式性价比差, 等 IV 回落
        result["signal"] = "WAIT_IV_HIGH"
        result["confidence"] = None
    elif delta_a > 0.6 and iv_low:
        # 分形强 + IV 便宜 = 最强信号
        result["signal"] = "BUY_STRADDLE_STRONG"
        result["confidence"] = "MEDIUM"
    elif delta_a > 0.6:
        # 分形强但 IV 中位
        result["signal"] = "BUY_STRADDLE_WEAK"
        result["confidence"] = "LOW"
    elif iv_low:
        # 分形弱但 IV 低位, 单纯买便宜期权
        result["signal"] = "BUY_STRADDLE_VOL_ONLY"
        result["confidence"] = "LOW"
    elif iv_high:
        # 分形弱 + IV 高位 → 考虑卖方 (但卖跨式风险无限, 不在此脚本范围)
        result["signal"] = "WAIT_IV_HIGH_SELL_CANDIDATE"
        result["confidence"] = None
    else:
        result["signal"] = "WAIT"
        result["confidence"] = None

    return result


def run(watchlist: list[str], days_to_expiry: int = 21) -> None:
    hc = health_check()
    if not hc.get("qot_logined"):
        print("Futu OpenD 未登录")
        return

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n{'═' * 88}")
    print(f"  跨式策略推荐  {ts}   US 市场: {hc.get('market_us','?')}")
    print(f"  目标到期天数: {days_to_expiry}")
    print(f"{'═' * 88}")

    for code in watchlist:
        try:
            feat = analyze_stock(code)
            # 取目标天数的 ATM 期权链 (±3% 行权价)
            atm = find_atm_options(code, days_to_expiry=days_to_expiry, strike_band=0.03)
            if atm.empty:
                print(f"\n  {code}: ATM 链为空, 跳过")
                continue
            sc = score_straddle_opportunity(feat, atm)

            print(f"\n{'─' * 88}")
            print(f"  {code}   现价=${feat['spot']:.2f}")
            print(f"  分形: asym={feat.get('asym', 0):+.3f}  Δα={feat.get('delta_alpha', 0):.3f}  h(q=2)={feat.get('hq2', 0):+.3f}")
            print(f"  波动: 实际年化={feat['realized_vol']:.1f}%  IV均值={sc.get('iv_avg', 0):.1f}%  IV Rank: {sc.get('iv_rank_desc', '-')}")
            print(f"  信号: {sc['signal']}  置信度: {sc.get('confidence', '-')}")

            if sc["signal"].startswith("BUY_STRADDLE"):
                print(f"")
                print(f"  跨式构造:")
                print(f"    Call: {sc['call_code']} @ ${sc['call_premium']:.2f}")
                print(f"    Put:  {sc['put_code']} @ ${sc['put_premium']:.2f}")
                print(f"    总成本/张: ${sc['straddle_cost']:.2f}  名义金额: ${sc['straddle_cost']*100:.0f}")
                print(f"    盈亏平衡: ${sc['breakeven_lower']:.2f} 以下 或 ${sc['breakeven_upper']:.2f} 以上")
                print(f"    需要 {sc['days_to_expiry']} 天内股价变动 ≥{sc['move_needed_pct']:.2f}%")
                print(f"")
                print(f"  下单命令 (两腿各执行一次, 各 1 张):")
                print(f"    ./venv/bin/python trade_futu_sim.py buy {sc['call_code']} 1 --confirm")
                print(f"    ./venv/bin/python trade_futu_sim.py buy {sc['put_code']} 1 --confirm")
            else:
                print(f"    → {sc['signal']}, 无跨式机会")
        except Exception as e:
            print(f"\n  {code}: 错误 {str(e)[:80]}")

    print(f"\n{'═' * 88}")
    print("  ⚠ 跨式需要大幅度波动才盈利. 若股价横盘, theta 衰减会双倍损失.")
    print("  ⚠ IV 便宜 vs 贵是相对判断, 此脚本用实际波动率作代理, 不如IV历史百分位准.")
    print(f"{'═' * 88}\n")


if __name__ == "__main__":
    wl_name = sys.argv[1] if len(sys.argv) > 1 else "default"
    if wl_name not in WATCHLISTS:
        print(f"可用: {list(WATCHLISTS)}")
        sys.exit(1)
    run(WATCHLISTS[wl_name], days_to_expiry=21)
