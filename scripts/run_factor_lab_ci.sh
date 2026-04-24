#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <market:a|us> [csv_prefix] [extra factor_lab args...]" >&2
  exit 1
fi

MARKET="$1"
shift

CSV_PREFIX="${1:-factor_candidate}"
if [[ $# -gt 0 ]]; then
  shift
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

FACTOR_STATUSES="${FACTOR_STATUSES:-candidate watch}"
read -r -a STATUS_ARGS <<< "${FACTOR_STATUSES}"

PY="${PYTHON:-python3}"
exec "$PY" manage.py factor-lab \
  --market "${MARKET}" \
  --csv-prefix "${CSV_PREFIX}" \
  --statuses "${STATUS_ARGS[@]}" \
  "$@"
