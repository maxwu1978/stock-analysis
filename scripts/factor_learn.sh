#!/usr/bin/env bash
set -euo pipefail

export NONINTERACTIVE=1
export PYTHONUNBUFFERED=1
export MPLBACKEND=Agg
export GIT_TERMINAL_PROMPT=0
export PIP_NO_INPUT=1

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

PY="${PYTHON:-python3}"
exec "$PY" manage.py factor-learn "$@"
