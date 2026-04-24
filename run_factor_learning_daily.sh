#!/bin/bash
# Daily factor-learning wrapper for launchd.
#
# Default workflow:
# - Run factor_learning.py for 60 minutes
# - Avoid overlapping runs with a local lock
# - Append all output to factor_learning_daily.log

set -euo pipefail

export NONINTERACTIVE=1
export PYTHONUNBUFFERED=1
export MPLBACKEND=Agg
export GIT_TERMINAL_PROMPT=0
export PIP_NO_INPUT=1

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$PROJECT_DIR/factor_learning_daily.log"
LOCK_DIR="$PROJECT_DIR/.factor_learning.lock"

if [ -x "$PROJECT_DIR/venv/bin/python" ]; then
    PY="$PROJECT_DIR/venv/bin/python"
else
    PY="python3"
fi

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "$(date '+%Y-%m-%d %H:%M:%S %Z') factor learning already running; skip" >> "$LOG_FILE"
    exit 0
fi
trap 'rm -rf "$LOCK_DIR"' EXIT

cd "$PROJECT_DIR"

DURATION_MIN="${FACTOR_LEARN_DURATION_MIN:-60}"
EXTRA_ARGS=()
if [ -n "${FACTOR_LEARN_EXTRA_ARGS:-}" ]; then
    read -r -a EXTRA_ARGS <<< "$FACTOR_LEARN_EXTRA_ARGS"
fi

{
    echo ""
    echo "═══════════════════════════════════════════════════════════════════════"
    echo "  Factor Learning Daily  $(date '+%Y-%m-%d %H:%M:%S %Z')"
    echo "═══════════════════════════════════════════════════════════════════════"
    echo "duration_min=$DURATION_MIN"

    "$PY" manage.py factor-learn \
        --duration-min "$DURATION_MIN" \
        "${EXTRA_ARGS[@]}"

    echo "completed_at=$(date '+%Y-%m-%d %H:%M:%S %Z')"
} >> "$LOG_FILE" 2>&1
