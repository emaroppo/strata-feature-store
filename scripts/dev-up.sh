#!/usr/bin/env bash
# Bring the local stack up and bootstrap it. Safe to re-run.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose up -d --wait
exec ./scripts/garage-bootstrap.sh
