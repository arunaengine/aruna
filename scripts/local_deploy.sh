#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/scripts/compose.yaml"
COMPOSE_PROJECT_NAME="${ARUNA_COMPOSE_PROJECT_NAME:-$(basename "$ROOT_DIR")}"
COMPOSE_DATA_DIR="${ARUNA_COMPOSE_DATA_DIR:-$ROOT_DIR/target/compose/node/storage}"
OIDC_USERNAME="${ARUNA_COMPOSE_OIDC_USERNAME:-aruna-admin}"
OIDC_PASSWORD="${ARUNA_COMPOSE_OIDC_PASSWORD:-aruna-admin}"
OIDC_SCOPE="${ARUNA_COMPOSE_OIDC_SCOPE:-openid profile}"
READY_TIMEOUT_SECS="${ARUNA_COMPOSE_READY_TIMEOUT_SECS:-120}"
FRESH=0
ADMIN_TOKEN=""

log() {
  printf '==> %s\n' "$*" >&2
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage: bash scripts/bootstrap_compose.sh [--new]

Behavior:
  default Reuse the mounted state directory in target/compose/node/storage.
          If the directory does not contain an existing database, bootstrap a fresh node in place.
  --new Clear the mounted state directory before starting, then bootstrap a fresh node.

Environment overrides:
  ARUNA_COMPOSE_PROJECT_NAME
  ARUNA_COMPOSE_DATA_DIR
  ARUNA_COMPOSE_OIDC_USERNAME
  ARUNA_COMPOSE_OIDC_PASSWORD
  ARUNA_COMPOSE_OIDC_SCOPE
  ARUNA_COMPOSE_READY_TIMEOUT_SECS
EOF
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

compose() {
  docker compose \
    --file "$COMPOSE_FILE" \
    --project-name "$COMPOSE_PROJECT_NAME" \
    "$@"
}

clear_compose_data_dir() {
  docker run --rm \
    -v "$COMPOSE_DATA_DIR:/data" \
    alpine:3.23 \
    sh -c 'rm -rf /data/* /data/.[!.]* /data/..?* 2>/dev/null || true'
}

compose_database_exists() {
  local journal

  if [[ -f "$COMPOSE_DATA_DIR/version" || -d "$COMPOSE_DATA_DIR/keyspaces" ]]; then
    return 0
  fi

  shopt -s nullglob
  for journal in "$COMPOSE_DATA_DIR"/*.jnl; do
    shopt -u nullglob
    return 0
  done
  shopt -u nullglob

  return 1
}

wait_for_url() {
  local name=$1
  local url=$2
  local deadline=$((SECONDS + READY_TIMEOUT_SECS))

  until curl --silent --show-error --fail --output /dev/null "$url"
  do
    if ((SECONDS >= deadline)); then
      die "timed out waiting for ${name} at ${url}"
    fi
    sleep 1
  done
}

strip_ansi_sequences() {
  local value=$1
  local esc=$'\033'
  local prefix
  local rest

  while [[ "$value" == *"$esc["* ]]; do
    prefix="${value%%"$esc["*}"
    rest="${value#*"$esc["}"

    if [[ "$rest" != *[[:alpha:]]* ]]; then
      break
    fi

    rest="${rest#*[[:alpha:]]}"
    value="${prefix}${rest}"
  done

  printf '%s\n' "$value"
}

wait_for_initial_onboarding_secret() {
  local deadline=$((SECONDS + READY_TIMEOUT_SECS))
  local secret
  local logs
  local line
  local plain_line

  while true; do
    logs="$(compose logs --no-color --tail 200 aruna 2>/dev/null || true)"
    secret=""

    while IFS= read -r line; do
      plain_line="$(strip_ansi_sequences "$line")"

      case "$plain_line" in
        *onboarding_secret=*)
          secret="${plain_line#*onboarding_secret=}"
          secret="${secret%%[[:space:]]*}"
          ;;
      esac
    done <<<"$logs"

    if [[ -n "$secret" ]]; then
      printf '%s\n' "$secret"
      return 0
    fi

    if ((SECONDS >= deadline)); then
      die "timed out waiting for the initial onboarding secret"
    fi
    sleep 1
  done
}

create_admin_token() {
  if (($# > 1)); then
    die "create_admin_token accepts at most one bootstrap secret"
  fi

  local args=(
    exec -T aruna /run/aruna-doctor create-token
    --oidc-username "$OIDC_USERNAME"
    --oidc-password "$OIDC_PASSWORD"
    --oidc-scope "$OIDC_SCOPE"
  )

  if (($# == 1)); then
    args+=(--bootstrap-secret "$1")
  fi

  compose "${args[@]}"
}

build_image() {
  log "Building aruna image"
  compose build aruna
}

stop_stack() {
  compose down --remove-orphans >/dev/null 2>&1 || true
}

start_stack() {
  log "Starting compose stack"
  compose up -d
}

wait_until_ready() {
  wait_for_url "Aruna API" "http://127.0.0.1:3000/swagger-ui"
}

bootstrap_fresh_state() {
  local bootstrap_secret
  local token

  start_stack
  wait_until_ready

  log "Reading initial onboarding secret"
  if ! bootstrap_secret="$(wait_for_initial_onboarding_secret)"; then
    die "failed to read the initial onboarding secret"
  fi

  log "Bootstrapping initial admin user via OIDC"
  if ! token="$(create_admin_token "$bootstrap_secret")"; then
    die "failed to bootstrap the initial admin user"
  fi

  ADMIN_TOKEN="$token"
}

bootstrap_existing_state() {
  local token

  start_stack
  wait_until_ready

  log "Creating admin token from mounted state"
  if ! token="$(create_admin_token)"; then
    die "failed to create an admin token from mounted state; run with --new to rebootstrap the local compose state"
  fi

  ADMIN_TOKEN="$token"
}

while (($# > 0)); do
  case "$1" in
    --new)
      FRESH=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
  shift
done

require_command docker
require_command curl

mkdir -p "$COMPOSE_DATA_DIR"
COMPOSE_DATA_DIR="$(cd "$COMPOSE_DATA_DIR" && pwd)"
export ARUNA_COMPOSE_DATA_DIR="$COMPOSE_DATA_DIR"

build_image
stop_stack

if ((FRESH)); then
  log "Clearing mounted compose state at $COMPOSE_DATA_DIR"
  clear_compose_data_dir
fi

if compose_database_exists; then
  bootstrap_existing_state
else
  if (( ! FRESH )); then
    log "No existing compose database found at $COMPOSE_DATA_DIR; bootstrapping a fresh node"
  fi
  bootstrap_fresh_state
fi

log "Aruna is ready at http://127.0.0.1:3000/swagger-ui"
log "Keycloak is ready at http://127.0.0.1:8080/realms/aruna/account"
log "Compose state directory: $COMPOSE_DATA_DIR"
printf 'ADMIN_TOKEN=%s\n' "$ADMIN_TOKEN"
