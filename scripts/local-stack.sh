#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export SPARKOS_REPO_ROOT="$ROOT_DIR"
cd "$ROOT_DIR"

COMPOSE_FILE="docker-compose.local.yaml"
ACTION="${1:-up}"
SPARK_SERVICES=(spark-master spark-worker spark-history)

compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose -f "$COMPOSE_FILE" "$@"
  else
    docker-compose -f "$COMPOSE_FILE" "$@"
  fi
}

init_hive() {
  if ! docker ps --format '{{.Names}}' | grep -q '^sparkos-hive$'; then
    echo "Hive container is not running."
    return 1
  fi
  echo "Waiting for HiveServer2..."
  for _ in $(seq 1 60); do
    if docker exec sparkos-hive /opt/hive/bin/beeline \
      -u jdbc:hive2://localhost:10000 \
      -e "show databases" >/dev/null 2>&1; then
      docker cp data/hive-demo.sql sparkos-hive:/tmp/hive-demo.sql
      docker exec sparkos-hive /opt/hive/bin/beeline \
        -u jdbc:hive2://localhost:10000 \
        -f /tmp/hive-demo.sql
      return 0
    fi
    sleep 2
  done
  echo "HiveServer2 did not become ready in time."
  return 1
}

case "$ACTION" in
  up)
    mkdir -p artifacts/spark-events data
    compose up -d "${SPARK_SERVICES[@]}"
    cat <<'EOF'
Local stack is starting.

Spark Master UI:  http://localhost:8080
Spark Worker UI:  http://localhost:8081
Spark History:    http://localhost:18080

Use:
  bash scripts/run-tui.sh config/local.yaml

Optional Hive:
  bash scripts/local-stack.sh hive-up
EOF
    ;;
  hive-up)
    compose up -d hive
    init_hive || true
    ;;
  init)
    init_hive
    ;;
  down)
    compose down
    ;;
  restart)
    compose down
    mkdir -p artifacts/spark-events data
    compose up -d "${SPARK_SERVICES[@]}"
    ;;
  status)
    compose ps
    ;;
  *)
    echo "Usage: bash scripts/local-stack.sh [up|hive-up|init|down|restart|status]"
    exit 1
    ;;
esac
