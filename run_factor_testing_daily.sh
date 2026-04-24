#!/bin/bash
# Daily candidate-factor testing wrapper for launchd.
#
# Default workflow:
# - Run factor_testing.py against candidate/watch/trial factors
# - Promote qualifying factors to trial
# - Emit factor_runtime_overlay.json for runtime scoring

set -euo pipefail

export NONINTERACTIVE=1
export PYTHONUNBUFFERED=1
export MPLBACKEND=Agg
export GIT_TERMINAL_PROMPT=0
export PIP_NO_INPUT=1

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$PROJECT_DIR/factor_testing_daily.log"
LOCK_DIR="$PROJECT_DIR/.factor_testing.lock"

if [ -x "$PROJECT_DIR/venv/bin/python" ]; then
    PY="$PROJECT_DIR/venv/bin/python"
else
    PY="python3"
fi

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "$(date '+%Y-%m-%d %H:%M:%S %Z') factor testing already running; skip" >> "$LOG_FILE"
    exit 0
fi
trap 'rm -rf "$LOCK_DIR"' EXIT

cd "$PROJECT_DIR"

EXTRA_ARGS=()
if [ -n "${FACTOR_TEST_EXTRA_ARGS:-}" ]; then
    read -r -a EXTRA_ARGS <<< "$FACTOR_TEST_EXTRA_ARGS"
fi
if [ "${FACTOR_TEST_DRY_RUN:-0}" = "1" ]; then
    EXTRA_ARGS+=("--dry-run")
fi
if [ "${FACTOR_TEST_APPLY_REJECTIONS:-0}" = "1" ]; then
    EXTRA_ARGS+=("--apply-rejections")
fi

{
    echo ""
    echo "═══════════════════════════════════════════════════════════════════════"
    echo "  Factor Testing Daily  $(date '+%Y-%m-%d %H:%M:%S %Z')"
    echo "═══════════════════════════════════════════════════════════════════════"

    "$PY" manage.py factor-test "${EXTRA_ARGS[@]}"

    if [ "${FACTOR_REGENERATE_PAGE:-0}" = "1" ]; then
        "$PY" generate_page.py
    fi

    echo "completed_at=$(date '+%Y-%m-%d %H:%M:%S %Z')"
} >> "$LOG_FILE" 2>&1
