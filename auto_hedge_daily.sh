#!/bin/bash
# 每日美股盘前自动挂 DAY 止盈单 (模拟盘 GTC 不支持)
#
# 流程:
#   1. 读当前期权持仓
#   2. 对每个持仓期权计算建议止盈价 (默认 +30%)
#   3. 挂 DAY 限价卖单 (当日有效)
#   4. 收盘时未成交会被富途自动撤销
#   5. 明天此脚本再跑一次, 重新挂

set -euo pipefail

PROJECT_DIR="/Volumes/MaxRelocated/主力分析"
VENV_PY="$PROJECT_DIR/venv/bin/python"
LOG="$PROJECT_DIR/auto_hedge.log"

cd "$PROJECT_DIR"

echo "" >> "$LOG"
echo "=== $(date +'%Y-%m-%d %H:%M:%S %Z') 每日自动挂单 ===" >> "$LOG"

# OpenD 检查
if ! lsof -iTCP:11111 -sTCP:LISTEN >/dev/null 2>&1; then
    echo "  [!] OpenD 未启动, 跳过" >> "$LOG"
    exit 0
fi

# 用 Python 读持仓并生成+执行挂单命令
"$VENV_PY" - >> "$LOG" 2>&1 <<'PYEOF'
import logging
logging.getLogger('futu').setLevel(logging.ERROR)

import re
import subprocess
from fetch_futu import get_positions
from futu import OpenSecTradeContext, RET_OK, TrdEnv, TrdMarket, SecurityFirm

TP_PCT = 0.30  # 止盈百分比

# 持仓
pos = get_positions(trd_env="SIMULATE")
if pos.empty or "qty" not in pos.columns:
    print("  无持仓或查询失败")
    exit(0)

opt_pattern = re.compile(r'^(US|HK)\.\w+\d{6}[CP]\d+$')
options = pos[pos["qty"] > 0].copy()
options = options[options["code"].str.match(opt_pattern)]

if options.empty:
    print("  当前无期权多头持仓")
    exit(0)

# 查今日已挂单 (未成交的 SELL 挂单), 避免重复
t = OpenSecTradeContext(
    filter_trdmarket=TrdMarket.US, host="127.0.0.1", port=11111,
    security_firm=SecurityFirm.FUTUSECURITIES,
)
try:
    ret, orders = t.order_list_query(trd_env=TrdEnv.SIMULATE)
    pending_sell_codes = set()
    if ret == RET_OK and not orders.empty:
        # 未成交的 SELL 单 (SUBMITTED / SUBMITTING / WAITING 等)
        active_statuses = {"SUBMITTED", "SUBMITTING", "WAITING"}
        mask = (
            (orders["trd_side"] == "SELL") &
            (orders["order_status"].isin(active_statuses))
        )
        pending_sell_codes = set(orders[mask]["code"].tolist())
finally:
    t.close()

print(f"  {len(options)} 个期权持仓, 已挂单 {len(pending_sell_codes)} 个")

for _, r in options.iterrows():
    code = r["code"]
    qty = int(r["qty"])
    cost = float(r["cost_price"])
    target_price = round(cost * (1 + TP_PCT), 2)

    if code in pending_sell_codes:
        print(f"    {code}: 已有未成交卖单, 跳过")
        continue

    cmd = [
        "./venv/bin/python", "trade_futu_sim.py",
        "limit_sell", code, str(qty), str(target_price), "--confirm",
    ]
    print(f"    {code} × {qty}张  成本${cost:.2f}  挂@${target_price:.2f} (+{TP_PCT*100:.0f}%)")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        out = (result.stdout + result.stderr).strip().split("\n")
        for line in out:
            if "✓" in line or "✗" in line:
                print(f"      {line.strip()}")
    except Exception as e:
        print(f"      [!] 失败: {e}")

print("  完成")
PYEOF

echo "" >> "$LOG"
tail -20 "$LOG"
