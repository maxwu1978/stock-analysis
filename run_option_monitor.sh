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

# 跑监控 (写片段 option_section.html, 发通知, 生成 text log)
"$VENV_PY" option_monitor.py --market-open-only 2>/dev/null \
    | grep -v -E "open_context_base|_init_connect_sync|on_disconnect|DeprecationWarning" \
    >> option_monitor_cron.log

# 如果片段更新了, 重新生成主页 docs/index.html 并 push
if [ -f "option_section.html" ]; then
    # 检查片段有无变化 (基于 git)
    if git diff --quiet option_section.html 2>/dev/null; then
        # 无变化 (盘外时也可能已经没有新内容)
        :
    else
        # 有变化, 重新生成主页
        "$VENV_PY" generate_page.py >> option_monitor_cron.log 2>&1 || \
            echo "$(date +%H:%M) generate_page failed" >> option_monitor_cron.log

        # 提交 + 推送
        git add docs/index.html option_section.html 2>/dev/null || true
        if ! git diff --cached --quiet 2>/dev/null; then
            git commit -q -m "主页期权持仓更新 $(date +%H:%M)" \
                -m "auto by option_monitor launchd job"
            git push origin main -q 2>&1 >> option_monitor_cron.log || \
                echo "$(date +%H:%M) git push failed" >> option_monitor_cron.log
        fi
    fi
fi
