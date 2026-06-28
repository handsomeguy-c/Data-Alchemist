#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

python3 -m venv .venv
. .venv/bin/activate

python -m pip install --upgrade pip setuptools wheel
python -m pip install -e ".[dev]"

echo
echo "AGI-吉尔伽美什 is ready."
echo "Run plain mode: .venv/bin/python -m sparkos.cli --plain"
echo "Run TUI mode:   .venv/bin/sparkos"
