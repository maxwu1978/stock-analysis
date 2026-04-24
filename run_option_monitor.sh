#!/bin/bash
# 期权持仓监控包装脚本 (供 launchd 调用)
# 工作流: 拉持仓 → 算PnL → 写 HTML → 如果有变化 git push
# 盘外时 option_monitor.py --market-open-only 自带跳过

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -x "$PROJECT_DIR/venv/bin/python" ]; then
    VENV_PY="$PROJECT_DIR/venv/bin/python"
else
    VENV_PY="python3"
fi

cd "$PROJECT_DIR"

file_hash() {
    if [ -f "$1" ]; then
        shasum -a 256 "$1" | awk '{print $1}'
    else
        echo "missing"
    fi
}

# 检查 OpenD 在线
if ! lsof -iTCP:11111 -sTCP:LISTEN >/dev/null 2>&1; then
    echo "$(date +%H:%M) OpenD 未启动, 跳过" >> option_monitor_cron.log
    exit 0
fi

OPTION_BEFORE="$(file_hash option_section.html)"

# 1) 期权持仓监控 (模拟盘, 写 option_section.html)
"$VENV_PY" manage.py option-monitor --market-open-only 2>/dev/null \
    | grep -v -E "open_context_base|_init_connect_sync|on_disconnect|DeprecationWarning" \
    >> option_monitor_cron.log

# 2) 期权链/IV 快照积累 (用于后续真实链路回测, 失败不影响页面刷新)
"$VENV_PY" manage.py option-chain-snapshot --watchlist tech --dtes 14,21 --sleep-sec 3.2 2>/dev/null \
    | grep -v -E "open_context_base|_init_connect_sync|on_disconnect|DeprecationWarning" \
    >> option_monitor_cron.log || \
    echo "$(date +%H:%M) option_chain_snapshot skipped/failed" >> option_monitor_cron.log

# 3) 真实盘观察 (只读, 写 real_position_section.html)
# 不用 --market-open-only, 真实持仓任何时候都值得观察
"$VENV_PY" manage.py real-position-observer 2>/dev/null \
    | grep -v -E "open_context_base|_init_connect_sync|on_disconnect|DeprecationWarning" \
    >> option_monitor_cron.log

# 4) 如果公开期权片段有变化, 重新生成公开 docs + push.
# option_section.html 是本地忽略文件, 不能用 git diff 检测。
OPTION_AFTER="$(file_hash option_section.html)"
ANY_CHANGE=0
if [ "$OPTION_BEFORE" != "$OPTION_AFTER" ]; then
    ANY_CHANGE=1
fi

if [ "$ANY_CHANGE" = "1" ]; then
    "$VENV_PY" manage.py refresh-page --strict >> option_monitor_cron.log 2>&1 || \
        echo "$(date +%H:%M) generate_page failed" >> option_monitor_cron.log

    git add docs/ 2>/dev/null || true
    if ! git diff --cached --quiet 2>/dev/null; then
        git commit -q -m "主页期权+真实盘更新 $(date +%H:%M)" \
            -m "auto by option_monitor launchd job"
        git push origin main -q 2>&1 >> option_monitor_cron.log || \
            echo "$(date +%H:%M) git push failed" >> option_monitor_cron.log
    fi
else
    echo "$(date +%H:%M) option fragment unchanged; skip docs refresh" >> option_monitor_cron.log
fi
