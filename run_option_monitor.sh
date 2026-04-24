#!/bin/bash
# 期权持仓监控包装脚本 (供 launchd 调用)
# 工作流: 拉持仓 → 算PnL → 写 HTML → 如果有变化 git push
# 盘外时 option_monitor.py --market-open-only 自带跳过

set -euo pipefail

PROJECT_DIR="/Volumes/MaxRelocated/主力分析"
VENV_PY="$PROJECT_DIR/venv/bin/python"

cd "$PROJECT_DIR"

# 检查 OpenD 在线
if ! lsof -iTCP:11111 -sTCP:LISTEN >/dev/null 2>&1; then
    echo "$(date +%H:%M) OpenD 未启动, 跳过" >> option_monitor_cron.log
    exit 0
fi

# 1) 期权持仓监控 (模拟盘, 写 option_section.html)
"$VENV_PY" option_monitor.py --market-open-only 2>/dev/null \
    | grep -v -E "open_context_base|_init_connect_sync|on_disconnect|DeprecationWarning" \
    >> option_monitor_cron.log

# 2) 期权链/IV 快照积累 (用于后续真实链路回测, 失败不影响页面刷新)
"$VENV_PY" option_chain_snapshot.py --watchlist tech --dtes 14,21 --sleep-sec 3.2 2>/dev/null \
    | grep -v -E "open_context_base|_init_connect_sync|on_disconnect|DeprecationWarning" \
    >> option_monitor_cron.log || \
    echo "$(date +%H:%M) option_chain_snapshot skipped/failed" >> option_monitor_cron.log

# 3) 真实盘观察 (只读, 写 real_position_section.html)
# 不用 --market-open-only, 真实持仓任何时候都值得观察
"$VENV_PY" real_position_observer.py 2>/dev/null \
    | grep -v -E "open_context_base|_init_connect_sync|on_disconnect|DeprecationWarning" \
    >> option_monitor_cron.log

# 4) 如果任一片段有变化, 重新生成主页 + push
ANY_CHANGE=0
for frag in option_section.html real_position_section.html; do
    if [ -f "$frag" ] && ! git diff --quiet "$frag" 2>/dev/null; then
        ANY_CHANGE=1
    fi
done

if [ "$ANY_CHANGE" = "1" ]; then
    "$VENV_PY" generate_page.py >> option_monitor_cron.log 2>&1 || \
        echo "$(date +%H:%M) generate_page failed" >> option_monitor_cron.log

    git add docs/index.html option_section.html real_position_section.html 2>/dev/null || true
    if ! git diff --cached --quiet 2>/dev/null; then
        git commit -q -m "主页期权+真实盘更新 $(date +%H:%M)" \
            -m "auto by option_monitor launchd job"
        git push origin main -q 2>&1 >> option_monitor_cron.log || \
            echo "$(date +%H:%M) git push failed" >> option_monitor_cron.log
    fi
fi
