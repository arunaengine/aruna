#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/scripts/compose.yaml"
COMPOSE_ENV_FILE="${ARUNA_COMPOSE_ENV_FILE:-$ROOT_DIR/scripts/compose.aruna.env}"
COMPOSE_PROJECT_NAME="${ARUNA_COMPOSE_PROJECT_NAME:-$(basename "$ROOT_DIR")}"
COMPOSE_DATA_DIR="${ARUNA_COMPOSE_DATA_DIR:-$ROOT_DIR/target/compose/node/storage}"
ARUNA_IMAGE="aruna:latest"
OIDC_USERNAME="${ARUNA_COMPOSE_OIDC_USERNAME:-aruna-admin}"
OIDC_PASSWORD="${ARUNA_COMPOSE_OIDC_PASSWORD:-aruna-admin}"
OIDC_SCOPE="${ARUNA_COMPOSE_OIDC_SCOPE:-openid profile}"
READY_TIMEOUT_SECS="${ARUNA_COMPOSE_READY_TIMEOUT_SECS:-120}"
# Strictly above the ops listener's five-second whole-request deadline, so a
# slow readiness probe answers instead of being cut off by the client.
READY_ATTEMPT_TIMEOUT_SECS="${ARUNA_COMPOSE_READY_ATTEMPT_TIMEOUT_SECS:-6}"
SKIP_BUILD="${ARUNA_COMPOSE_SKIP_BUILD:-0}"
KEYCLOAK_PORT="${ARUNA_KEYCLOAK_PORT:-8080}"
LOG_TAIL_LINES=50
FRESH=0
ADMIN_TOKEN=""
LAST_READY_CODE=""
LAST_READY_BODY=""

log() {
  printf '==> %s\n' "$*" >&2
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage: bash scripts/local_deploy.sh [--new] [--help]

Behavior:
  default Reuse the mounted state directory in target/compose/node/storage.
          If the directory does not contain an existing database, bootstrap a fresh node in place.
  --new   Clear the mounted state directory before starting, then bootstrap a fresh node.
  --help  Print this help and exit.

Readiness:
  Startup waits for /readyz on the ops port taken from the mounted compose env
  file (OPS_SOCKET_ADDRESS, 3002 by default). A 503 keeps waiting.

Environment overrides:
  ARUNA_COMPOSE_PROJECT_NAME
  ARUNA_COMPOSE_DATA_DIR
  ARUNA_COMPOSE_ENV_FILE
  ARUNA_COMPOSE_OIDC_USERNAME
  ARUNA_COMPOSE_OIDC_PASSWORD
  ARUNA_COMPOSE_OIDC_SCOPE
  ARUNA_COMPOSE_READY_TIMEOUT_SECS
  ARUNA_COMPOSE_READY_ATTEMPT_TIMEOUT_SECS
  ARUNA_COMPOSE_SKIP_BUILD          1 reuses an existing aruna:latest image
  ARUNA_KEYCLOAK_PORT               compose Keycloak port, 8080 by default
  ARUNA_PROMETHEUS_PORT
  ARUNA_PROMETHEUS_CONFIG
  ARUNA_GRAFANA_PORT
EOF
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

require_positive_int() {
  local name=$1
  local value=$2

  [[ "$value" =~ ^[1-9][0-9]*$ ]] || die "$name must be a positive integer, got: $value"
}

# Refuses to clear the filesystem root, the home directory, the repository, or
# any directory containing the repository.
assert_removable() {
  local dir=$1
  local resolved

  resolved="$(cd -- "$dir" 2>/dev/null && pwd -P)" || die "compose data directory not found: $dir"
  case "$resolved" in
    / | "${HOME:-}" | "$ROOT_DIR")
      die "refusing to clear $resolved; point ARUNA_COMPOSE_DATA_DIR at a dedicated directory"
      ;;
  esac
  [[ "$resolved" == /*/* ]] || die "refusing to clear the top-level directory $resolved"
  case "$ROOT_DIR/" in
    "$resolved"/*)
      die "refusing to clear $resolved because it contains the repository root"
      ;;
  esac
}

env_file_port() {
  local key=$1
  local fallback=$2
  local candidate
  local line=""
  local value

  if [[ -f "$COMPOSE_ENV_FILE" ]]; then
    while IFS= read -r candidate; do
      case "$candidate" in
        "$key"=*) line="$candidate" ;;
      esac
    done <"$COMPOSE_ENV_FILE"
  fi

  if [[ -z "$line" ]]; then
    printf '%s\n' "$fallback"
    return 0
  fi

  value="${line#*=}"
  value="${value##*:}"
  value="${value%%[[:space:]]*}"
  require_positive_int "$key in $COMPOSE_ENV_FILE" "$value"
  printf '%s\n' "$value"
}

compose() {
  docker compose \
    --file "$COMPOSE_FILE" \
    --project-name "$COMPOSE_PROJECT_NAME" \
    "$@"
}

service_logs() {
  compose logs --no-color --tail "$LOG_TAIL_LINES" aruna 2>&1 || true
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
  # shellcheck disable=SC2034 # loop variable only probes for a matching file
  for journal in "$COMPOSE_DATA_DIR"/*.jnl; do
    shopt -u nullglob
    return 0
  done
  shopt -u nullglob

  return 1
}

aruna_container_dead() {
  local exited

  exited="$(compose ps --status exited --quiet aruna 2>/dev/null || true)"
  [[ -n "$exited" ]]
}

probe_readiness() {
  local url=$1
  local response

  response="$(
    curl --silent --max-time "$READY_ATTEMPT_TIMEOUT_SECS" \
      --write-out $'\n%{http_code}' "$url" 2>/dev/null || true
  )"
  LAST_READY_CODE="${response##*$'\n'}"
  LAST_READY_BODY="${response%$'\n'*}"

  [[ "$LAST_READY_CODE" == "200" ]]
}

wait_until_ready() {
  local url=$1
  local deadline=$((SECONDS + READY_TIMEOUT_SECS))

  until probe_readiness "$url"; do
    if aruna_container_dead; then
      printf '%s\n' "$(service_logs)" >&2
      die "the aruna container exited before it became ready; inspect the log above"
    fi
    if ((SECONDS >= deadline)); then
      printf '%s\n' "$(service_logs)" >&2
      die "timed out waiting for readiness at ${url} (last status ${LAST_READY_CODE:-none}: ${LAST_READY_BODY:-no body})"
    fi
    sleep 1
  done
}

strip_ansi_sequences() {
  local value=$1
  local esc=$'\033'
  local prefix
  local rest

  while [[ "$value" == *"${esc}["* ]]; do
    prefix="${value%%"${esc}["*}"
    rest="${value#*"${esc}["}"

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
  if [[ "$SKIP_BUILD" == "1" ]]; then
    docker image inspect "$ARUNA_IMAGE" >/dev/null 2>&1 \
      || die "ARUNA_COMPOSE_SKIP_BUILD=1 but the $ARUNA_IMAGE image is missing; build it first"
    log "Reusing the existing $ARUNA_IMAGE image"
    return
  fi

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

bootstrap_fresh_state() {
  local bootstrap_secret
  local token

  start_stack
  wait_until_ready "$OPS_READY_URL"

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
  wait_until_ready "$OPS_READY_URL"

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

require_positive_int ARUNA_COMPOSE_READY_TIMEOUT_SECS "$READY_TIMEOUT_SECS"
require_positive_int ARUNA_COMPOSE_READY_ATTEMPT_TIMEOUT_SECS "$READY_ATTEMPT_TIMEOUT_SECS"
((READY_ATTEMPT_TIMEOUT_SECS >= 6)) \
  || die "ARUNA_COMPOSE_READY_ATTEMPT_TIMEOUT_SECS must be at least 6, above the ops request deadline"
[[ -f "$COMPOSE_ENV_FILE" ]] || die "compose env file not found: $COMPOSE_ENV_FILE"

require_command docker
require_command curl

mkdir -p "$COMPOSE_DATA_DIR"
COMPOSE_DATA_DIR="$(cd "$COMPOSE_DATA_DIR" && pwd)"
export ARUNA_COMPOSE_DATA_DIR="$COMPOSE_DATA_DIR"
export ARUNA_COMPOSE_ENV_FILE="$COMPOSE_ENV_FILE"

if ((FRESH)); then
  assert_removable "$COMPOSE_DATA_DIR"
fi

OPS_PORT="$(env_file_port OPS_SOCKET_ADDRESS 3002)"
REST_PORT="$(env_file_port SOCKET_ADDRESS 3000)"
OPS_READY_URL="http://127.0.0.1:$OPS_PORT/readyz"

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

log "Aruna is ready at http://127.0.0.1:$REST_PORT (ops readiness: $OPS_READY_URL)"
log "Swagger UI is at http://127.0.0.1:$REST_PORT/swagger-ui"
log "Keycloak is ready at http://127.0.0.1:$KEYCLOAK_PORT/realms/aruna/account"
log "Compose state directory: $COMPOSE_DATA_DIR"
printf 'ADMIN_TOKEN=%s\n' "$ADMIN_TOKEN"
