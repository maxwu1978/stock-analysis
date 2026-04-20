#!/bin/bash
# 注册 launchd 定时任务 (每工作日 15:00 + 22:00 CEST 运行 advisor)
# 需要 Futu OpenD 保持常驻并已登录/解锁
#
# 卸载: ./install_launchd.sh uninstall

set -euo pipefail

PLIST_SRC="/Volumes/MaxRelocated/主力分析/com.maxwu.fractal-advisor.plist"
PLIST_DST="$HOME/Library/LaunchAgents/com.maxwu.fractal-advisor.plist"
LABEL="com.maxwu.fractal-advisor"

cmd="${1:-install}"

case "$cmd" in
    install)
        mkdir -p "$HOME/Library/LaunchAgents"
        cp "$PLIST_SRC" "$PLIST_DST"
        # macOS 新版用 bootstrap 替代 load
        launchctl bootstrap "gui/$(id -u)" "$PLIST_DST" 2>/dev/null || launchctl load "$PLIST_DST"
        echo "✓ 已安装: $LABEL"
        echo "  下次运行: 工作日 15:00 或 22:00 CEST"
        echo "  日志: /Volumes/MaxRelocated/主力分析/advisor_history.log"
        launchctl list | grep "$LABEL" || echo "  (尚未在 list 中, 稍等几秒)"
        ;;
    uninstall)
        launchctl bootout "gui/$(id -u)" "$PLIST_DST" 2>/dev/null || launchctl unload "$PLIST_DST" 2>/dev/null || true
        rm -f "$PLIST_DST"
        echo "✓ 已卸载: $LABEL"
        ;;
    status)
        launchctl list | grep "$LABEL" || echo "未安装"
        ;;
    test)
        # 立即跑一次 (不等计划时间)
        /Volumes/MaxRelocated/主力分析/run_advisor_daily.sh
        echo "✓ 测试运行完成, 查看 advisor_history.log"
        ;;
    *)
        echo "用法: $0 [install|uninstall|status|test]"
        exit 1
        ;;
esac
