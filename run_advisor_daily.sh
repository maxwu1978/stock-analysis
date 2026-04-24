#!/bin/bash
# 每日分形期权策略推荐脚本 (供 launchd 定时调用)
#
# 运行要求: Futu OpenD 后台常驻并已登录 + 交易已解锁
# 输出: 追加到 advisor_history.log, 每次运行单独一个日期分块

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$PROJECT_DIR/advisor_history.log"
if [ -x "$PROJECT_DIR/venv/bin/python" ]; then
    VENV_PY="$PROJECT_DIR/venv/bin/python"
else
    VENV_PY="python3"
fi

cd "$PROJECT_DIR"

TS=$(date +"%Y-%m-%d %H:%M:%S %Z")

# 干净分离: stdout → 日志文件, stderr → 丢弃 (过滤 Futu SDK 的 debug 噪声)
{
    echo ""
    echo "═══════════════════════════════════════════════════════════════════════"
    echo "  运行时间: $TS"
    echo "═══════════════════════════════════════════════════════════════════════"

    # 先检查 OpenD 是否在线
    if ! lsof -iTCP:11111 -sTCP:LISTEN >/dev/null 2>&1; then
        echo "  [!] Futu OpenD 未启动 (端口 11111 无监听), 跳过本次运行"
        exit 0
    fi

    # 过滤掉 futu SDK 的 connect/disconnect 日志噪声
    "$VENV_PY" manage.py option-advisor tech_plus 2>/dev/null | \
        grep -v -E "open_context_base|_init_connect_sync|on_disconnect|DeprecationWarning|trace-deprecation" \
        || echo "  [!] advisor 脚本运行失败"

} 2>/dev/null >> "$LOG_FILE"
