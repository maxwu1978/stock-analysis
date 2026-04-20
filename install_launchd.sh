#!/bin/bash
# 注册 launchd 定时任务 (每工作日 15:00 + 22:00 CEST 运行 advisor)
# 需要 Futu OpenD 保持常驻并已登录/解锁
#
# 卸载: ./install_launchd.sh uninstall

set -euo pipefail

PROJECT_DIR="/Volumes/MaxRelocated/主力分析"
LAUNCH_AGENTS="$HOME/Library/LaunchAgents"

# 两个任务:
JOBS=(
    "com.maxwu.fractal-advisor"  # 工作日 15:00/22:00 跑策略推荐
    "com.maxwu.option-monitor"   # 每小时跑期权持仓监控 (自带市场过滤)
)

cmd="${1:-install}"

case "$cmd" in
    install)
        mkdir -p "$LAUNCH_AGENTS"
        for label in "${JOBS[@]}"; do
            src="$PROJECT_DIR/$label.plist"
            dst="$LAUNCH_AGENTS/$label.plist"
            if [ ! -f "$src" ]; then
                echo "  [!] 跳过 $label: $src 不存在"
                continue
            fi
            cp "$src" "$dst"
            launchctl bootstrap "gui/$(id -u)" "$dst" 2>/dev/null \
                || launchctl load "$dst"
            echo "✓ 已安装: $label"
        done
        echo ""
        echo "  策略推荐: 工作日 15:00 / 22:00 CEST → advisor_history.log"
        echo "  持仓监控: 每小时 (含盘前/中/后) → option_status.log + macOS 通知"
        echo ""
        echo "当前 launchctl 状态:"
        launchctl list | grep -E "fractal-advisor|option-monitor" || echo "  (稍等几秒后重试 status)"
        ;;
    uninstall)
        for label in "${JOBS[@]}"; do
            dst="$LAUNCH_AGENTS/$label.plist"
            launchctl bootout "gui/$(id -u)" "$dst" 2>/dev/null \
                || launchctl unload "$dst" 2>/dev/null || true
            rm -f "$dst"
            echo "✓ 已卸载: $label"
        done
        ;;
    status)
        echo "launchctl list 中的任务:"
        launchctl list | grep -E "fractal-advisor|option-monitor" || echo "  (未安装)"
        ;;
    test)
        echo "=== 测试 fractal-advisor ==="
        "$PROJECT_DIR/run_advisor_daily.sh"
        echo ""
        echo "=== 测试 option-monitor ==="
        "$PROJECT_DIR/venv/bin/python" "$PROJECT_DIR/option_monitor.py"
        ;;
    *)
        echo "用法: $0 [install|uninstall|status|test]"
        exit 1
        ;;
esac
