#!/usr/bin/env bash
# Stops whatever local_cluster_deploy.sh left running: the deploy script while
# it still monitors, every node named by a pid file under the deployment root,
# and the Keycloak compose project. Safe to run twice.
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
DEPLOY_ROOT="${ARUNA_TEST_DEPLOY_ROOT:-$ROOT_DIR/target/test-deploy}"
KEYCLOAK_PROJECT_NAME="${ARUNA_TEST_DEPLOY_KEYCLOAK_PROJECT:-aruna-test-deploy-oidc}"
KEYCLOAK_HTTP_PORT="${ARUNA_TEST_DEPLOY_KEYCLOAK_PORT:-43031}"
STOP_TIMEOUT_SECS="${ARUNA_TEST_DEPLOY_STOP_TIMEOUT_SECS:-20}"
STOPPED=0

usage() {
  cat <<'EOF'
Usage: bash scripts/local_cluster_stop.sh [--help]

Stops a local cluster started by scripts/local_cluster_deploy.sh (just local-cluster,
just local-cluster-oidc, just preview, just preview-no-oidc), in this order:

  1. a deploy script that is still monitoring the nodes, so its own cleanup runs
  2. every node named by a pid file under the deployment root, by SIGTERM and,
     after the stop timeout, SIGKILL; a pid that is not an aruna process is left alone
  3. the Keycloak compose project, with its volumes

Logs, summary.txt and credentials.txt stay in the deployment root.

Environment:
  ARUNA_TEST_DEPLOY_ROOT              deployment root, target/test-deploy by default
  ARUNA_TEST_DEPLOY_KEYCLOAK_PROJECT  compose project name, aruna-test-deploy-oidc by default
  ARUNA_TEST_DEPLOY_STOP_TIMEOUT_SECS seconds to wait per process before SIGKILL, 20 by default
EOF
}

log() {
  printf '[stop] %s\n' "$*" >&2
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

alive() {
  kill -0 "$1" >/dev/null 2>&1
}

# Waits for a process to leave, escalating to SIGKILL after the timeout.
stop_pid() {
  local pid=$1
  local name=$2
  local deadline=$((SECONDS + STOP_TIMEOUT_SECS))

  kill -TERM "$pid" >/dev/null 2>&1 || true
  while alive "$pid"; do
    if ((SECONDS >= deadline)); then
      log "$name (pid $pid) ignored SIGTERM for ${STOP_TIMEOUT_SECS}s; killing it"
      kill -KILL "$pid" >/dev/null 2>&1 || true
      break
    fi
    sleep 0.2
  done
  while alive "$pid"; do
    sleep 0.2
  done
  STOPPED=$((STOPPED + 1))
  log "Stopped $name (pid $pid)"
}

# A running deploy script tears its own nodes and Keycloak down on SIGINT.
stop_deploy_scripts() {
  local pid
  local deadline

  command -v pgrep >/dev/null 2>&1 || return 0
  for pid in $(pgrep -f 'bash .*scripts/local_cluster_deploy\.sh' || true); do
    [[ "$pid" != "$$" ]] || continue
    log "Interrupting the deploy script (pid $pid) so its cleanup runs"
    kill -INT "$pid" >/dev/null 2>&1 || true
    deadline=$((SECONDS + STOP_TIMEOUT_SECS))
    while alive "$pid" && ((SECONDS < deadline)); do
      sleep 0.2
    done
    if alive "$pid"; then
      stop_pid "$pid" "deploy script"
    fi
  done
}

# The pid file may outlive the node and the pid may have been reused, so only a
# process that still is an aruna node is stopped.
stop_nodes() {
  local pid_file
  local pid
  local name
  local comm

  for pid_file in "$DEPLOY_ROOT"/node-*/node-*.pid; do
    [[ -f "$pid_file" ]] || continue
    name="$(basename -- "$pid_file" .pid)"
    pid="$(tr -d '[:space:]' <"$pid_file")"
    if [[ ! "$pid" =~ ^[0-9]+$ ]]; then
      log "$name has an unreadable pid file; leaving it"
      continue
    fi
    if ! alive "$pid"; then
      log "$name (pid $pid) is already gone"
      rm -f -- "$pid_file"
      continue
    fi
    comm="$(ps -o comm= -p "$pid" 2>/dev/null || true)"
    if [[ "$comm" != aruna* ]]; then
      log "$name pid $pid now belongs to '${comm:-?}', not to a node; leaving it"
      continue
    fi
    stop_pid "$pid" "$name"
    rm -f -- "$pid_file"
  done
}

stop_keycloak() {
  if ! command -v docker >/dev/null 2>&1; then
    log "docker is not installed; no Keycloak to stop"
    return 0
  fi
  log "Removing the Keycloak compose project $KEYCLOAK_PROJECT_NAME"
  ARUNA_TEST_DEPLOY_KEYCLOAK_PORT="$KEYCLOAK_HTTP_PORT" docker compose \
    --project-name "$KEYCLOAK_PROJECT_NAME" \
    --file "$ROOT_DIR/scripts/keycloak/docker-compose.yml" \
    down --volumes --remove-orphans >/dev/null 2>&1 \
    || log "compose down failed for $KEYCLOAK_PROJECT_NAME; check docker compose ls"
}

while (($# > 0)); do
  case "$1" in
    --help | -h)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

stop_deploy_scripts
if [[ -d "$DEPLOY_ROOT" ]]; then
  stop_nodes
else
  log "No deployment root at $DEPLOY_ROOT"
fi
stop_keycloak
log "Done: $STOPPED process(es) stopped"
