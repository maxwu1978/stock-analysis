#!/bin/bash
# 注册 launchd 定时任务
# 需要 Futu OpenD 保持常驻并已登录/解锁
#
# 卸载: ./install_launchd.sh uninstall

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAUNCH_AGENTS="$HOME/Library/LaunchAgents"

# 两个任务:
JOBS=(
    "com.maxwu.fractal-advisor"  # 工作日 15:00/22:00 跑策略推荐
    "com.maxwu.option-monitor"   # 每小时跑期权持仓监控 (自带市场过滤)
    "com.maxwu.auto-hedge"       # 工作日 15:30 每日自动挂 DAY 止盈单
    "com.maxwu.factor-learning"  # 每天 12:10 本机时间学习候选因子
    "com.maxwu.factor-testing"   # 每天 13:25 本机时间测试并推送 trial 因子
)

FACTOR_JOBS=(
    "com.maxwu.factor-learning"
    "com.maxwu.factor-testing"
)

cmd="${1:-install}"

install_jobs() {
    mkdir -p "$LAUNCH_AGENTS"
    for label in "$@"; do
        src="$PROJECT_DIR/$label.plist"
        dst="$LAUNCH_AGENTS/$label.plist"
        if [ ! -f "$src" ]; then
            echo "  [!] 跳过 $label: $src 不存在"
            continue
        fi
        sed \
            -e "s#/Volumes/MaxRelocated/主力分析#$PROJECT_DIR#g" \
            -e "s#/Users/wuqingxin/Desktop/test/Program/9，主力分析#$PROJECT_DIR#g" \
            "$src" > "$dst"
        launchctl bootstrap "gui/$(id -u)" "$dst" 2>/dev/null \
            || launchctl load "$dst"
        echo "✓ 已安装: $label"
    done
}

uninstall_jobs() {
    for label in "$@"; do
        dst="$LAUNCH_AGENTS/$label.plist"
        launchctl bootout "gui/$(id -u)" "$dst" 2>/dev/null \
            || launchctl unload "$dst" 2>/dev/null || true
        rm -f "$dst"
        echo "✓ 已卸载: $label"
    done
}

case "$cmd" in
    install)
        install_jobs "${JOBS[@]}"
        echo ""
        echo "  策略推荐: 工作日 15:00 / 22:00 CEST → advisor_history.log"
        echo "  持仓监控: 每小时 (含盘前/中/后) → option_status.log + macOS 通知"
        echo "  因子学习: 每天 12:10 本机时间 → factor_learning_daily.log"
        echo "  因子测试: 每天 13:25 本机时间 → factor_testing_daily.log"
        echo ""
        echo "当前 launchctl 状态:"
        launchctl list | grep -E "fractal-advisor|option-monitor|auto-hedge|factor-learning|factor-testing" || echo "  (稍等几秒后重试 status)"
        ;;
    install-factors)
        install_jobs "${FACTOR_JOBS[@]}"
        echo ""
        echo "  因子学习: 每天 12:10 本机时间 → factor_learning_daily.log"
        echo "  因子测试: 每天 13:25 本机时间 → factor_testing_daily.log"
        echo ""
        launchctl list | grep -E "factor-learning|factor-testing" || echo "  (稍等几秒后重试 status)"
        ;;
    uninstall)
        uninstall_jobs "${JOBS[@]}"
        ;;
    uninstall-factors)
        uninstall_jobs "${FACTOR_JOBS[@]}"
        ;;
    status)
        echo "launchctl list 中的任务:"
        launchctl list | grep -E "fractal-advisor|option-monitor|auto-hedge|factor-learning|factor-testing" || echo "  (未安装)"
        ;;
    test)
        echo "=== 测试 fractal-advisor ==="
        "$PROJECT_DIR/run_advisor_daily.sh"
        echo ""
        echo "=== 测试 option-monitor ==="
        if [ -x "$PROJECT_DIR/venv/bin/python" ]; then
            PY="$PROJECT_DIR/venv/bin/python"
        else
            PY="python3"
        fi
        "$PY" "$PROJECT_DIR/manage.py" option-monitor
        echo ""
        echo "=== 测试 factor-learning (short dry run) ==="
        FACTOR_LEARN_DURATION_MIN=0 FACTOR_LEARN_EXTRA_ARGS="--max-cycles 1 --max-candidates 1 --limit-per-market 1 --no-import" \
            "$PROJECT_DIR/run_factor_learning_daily.sh"
        echo ""
        echo "=== 测试 factor-testing (short dry run) ==="
        FACTOR_TEST_DRY_RUN=1 FACTOR_TEST_EXTRA_ARGS="--max-candidates 1" \
            "$PROJECT_DIR/run_factor_testing_daily.sh"
        ;;
    *)
        echo "用法: $0 [install|install-factors|uninstall|uninstall-factors|status|test]"
        exit 1
        ;;
esac
