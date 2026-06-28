#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export SPARKOS_REPO_ROOT="$ROOT_DIR"
cd "$ROOT_DIR"

CONFIG_PATH="${1:-${SPARKOS_CONFIG:-config/config.yaml}}"

if [ ! -x ".venv/bin/python" ]; then
  echo "Missing .venv. Run: bash scripts/bootstrap.sh"
  exit 1
fi

if [ ! -f "$CONFIG_PATH" ]; then
  echo "Missing config file: $CONFIG_PATH"
  exit 1
fi

eval "$(.venv/bin/python scripts/load-config-env.py "$CONFIG_PATH")"

.venv/bin/python -m sparkos.cli --plain --config "$CONFIG_PATH"
