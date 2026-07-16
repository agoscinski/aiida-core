#!/usr/bin/env bash
set -euo pipefail

pixi run -e default python benchmark_daemon_configs.py \
  --storage-backend core.sqlite_dos \
  --number 800 \
  --workers 8 \
  --slots 100

pixi run -e default python benchmark_daemon_configs.py \
  --storage-backend core.turso_dos \
  --number 800 \
  --workers 8 \
  --slots 100

pixi run -e default python benchmark_daemon_configs.py \
  --storage-backend core.turso_dos \
  --number 8 \
  --workers 1 \
  --slots 10
