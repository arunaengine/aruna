#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
DEPLOY_ROOT="$ROOT_DIR/target/test-deploy"
ARUNA_BIN="$ROOT_DIR/target/release/aruna"
ARUNA_DOCTOR_BIN="$ROOT_DIR/target/release/aruna-doctor"
READY_TIMEOUT_SECS="${ARUNA_TEST_DEPLOY_READY_TIMEOUT_SECS:-90}"
EXIT_AFTER_READY="${ARUNA_TEST_DEPLOY_EXIT_AFTER_READY:-0}"
BASE_PORT="${ARUNA_TEST_DEPLOY_BASE_PORT:-43000}"
NODE_COUNT="${ARUNA_TEST_DEPLOY_NODE_COUNT:-3}"
KEYCLOAK_HTTP_PORT="${ARUNA_TEST_DEPLOY_KEYCLOAK_PORT:-}"
KEYCLOAK_PROJECT_NAME="${ARUNA_TEST_DEPLOY_KEYCLOAK_PROJECT:-aruna-test-deploy-oidc}"
KEYCLOAK_ADMIN_USER="${ARUNA_TEST_DEPLOY_KEYCLOAK_ADMIN_USER:-admin}"
KEYCLOAK_ADMIN_PASSWORD="${ARUNA_TEST_DEPLOY_KEYCLOAK_ADMIN_PASSWORD:-admin}"
KEYCLOAK_REALM="${ARUNA_TEST_DEPLOY_KEYCLOAK_REALM:-aruna}"
KEYCLOAK_CLIENT_ID="${ARUNA_TEST_DEPLOY_KEYCLOAK_CLIENT_ID:-aruna-api}"
KEYCLOAK_OIDC_USERNAME="${ARUNA_TEST_DEPLOY_OIDC_USERNAME:-aruna-admin}"
KEYCLOAK_OIDC_PASSWORD="${ARUNA_TEST_DEPLOY_OIDC_PASSWORD:-aruna-admin}"
WITH_KEYCLOAK=0
PIDS=()
NODE_NAMES=()
NODE_DIRS=()
NODE_BASE_URLS=()
NODE_HTTP_PORTS=()
NODE_P2P_PORTS=()
NODE_S3_PORTS=()
STARTED_PID=""

log() {
  printf '==> %s\n' "$*"
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage: bash scripts/local_cluster_deploy.sh [--with-keycloak] [--node-count N]

Behavior:
  default          Build the workspace in release mode and launch 3 local Aruna nodes.
  --with-keycloak  Start a local Keycloak instance and configure every node for OIDC.
  --node-count N   Launch N total Aruna nodes. Defaults to 3.

Environment overrides:
  ARUNA_TEST_DEPLOY_BASE_PORT
  ARUNA_TEST_DEPLOY_EXIT_AFTER_READY
  ARUNA_TEST_DEPLOY_NODE_COUNT
  ARUNA_TEST_DEPLOY_READY_TIMEOUT_SECS
  ARUNA_TEST_DEPLOY_KEYCLOAK_PORT
  ARUNA_TEST_DEPLOY_KEYCLOAK_PROJECT
  ARUNA_TEST_DEPLOY_KEYCLOAK_ADMIN_USER
  ARUNA_TEST_DEPLOY_KEYCLOAK_ADMIN_PASSWORD
  ARUNA_TEST_DEPLOY_KEYCLOAK_REALM
  ARUNA_TEST_DEPLOY_KEYCLOAK_CLIENT_ID
  ARUNA_TEST_DEPLOY_OIDC_USERNAME
  ARUNA_TEST_DEPLOY_OIDC_PASSWORD
EOF
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

cleanup() {
  local status=$?

  if ((${#PIDS[@]} > 0)); then
    for pid in "${PIDS[@]}"; do
      kill "$pid" >/dev/null 2>&1 || true
    done
    for pid in "${PIDS[@]}"; do
      wait "$pid" 2>/dev/null || true
    done
  fi

  if [[ "$WITH_KEYCLOAK" == "1" ]]; then
    ARUNA_TEST_DEPLOY_KEYCLOAK_PORT="$KEYCLOAK_HTTP_PORT" docker compose \
      --project-name "$KEYCLOAK_PROJECT_NAME" \
      --file "$ROOT_DIR/scripts/keycloak/docker-compose.yml" \
      down --volumes >/dev/null 2>&1 || true
  fi

  if [[ $status -ne 0 && $status -ne 130 ]]; then
    printf 'Deployment failed. Inspect logs in %s\n' "$DEPLOY_ROOT" >&2
  fi
}

handle_signal() {
  exit 130
}

assert_port_free() {
  local port=$1
  local listeners

  listeners="$(ss -ltnH "sport = :$port" || true)"
  [[ -z "$listeners" ]] || die "port $port is already in use; set ARUNA_TEST_DEPLOY_BASE_PORT to another range"
}

compact_json() {
  local json=$1

  json=${json//$'\n'/}
  json=${json//$'\r'/}
  json=${json//$'\t'/}
  json=${json// /}
  printf '%s\n' "$json"
}

json_string_field() {
  local json
  local key=$2
  local marker

  json="$(compact_json "$1")"
  marker="\"$key\":\""
  [[ "$json" == *"$marker"* ]] || die "missing JSON field: $key"
  json="${json#*"$marker"}"
  printf '%s\n' "${json%%\"*}"
}

write_node_env() {
  local node_dir=$1
  local http_port=$2
  local p2p_port=$3
  local s3_port=$4
  local onboarding_secret=${5:-}
  local max_concurrent_uni_streams="${MAX_CONCURRENT_UNI_STREAMS:-}"
  local max_concurrent_bidi_streams="${MAX_CONCURRENT_BIDI_STREAMS:-}"

  mkdir -p "$node_dir/storage" "$node_dir/blob"
  {
    printf 'STORAGE_PATH=%s\n' "$node_dir/storage"
    printf 'BLOB_ROOT=%s\n' "$node_dir/blob"
    printf 'BLOB_MULTIPART_BUCKET=%s\n' "parts"
    printf 'BLOB_MAX_BUCKET_SIZE=10000\n'
    printf 'SOCKET_ADDRESS=127.0.0.1:%s\n' "$http_port"
    printf 'P2P_SOCKET_ADDRESS=127.0.0.1:%s\n' "$p2p_port"
    printf 'S3_HOST=127.0.0.1:%s\n' "$s3_port"
    printf 'S3_ADDRESS=127.0.0.1:%s\n' "$s3_port"
    printf 'REALM_DESCRIPTION=Test_Deploy_Realm\n'
    printf 'METADATA_REPLICATION_FACTOR=3\n'
    if [[ "$WITH_KEYCLOAK" == "1" ]]; then
      printf 'OIDC_PROVIDER_IDS=main\n'
      printf 'OIDC_MAIN_ISSUER=%s\n' "$KEYCLOAK_ISSUER"
      printf 'OIDC_MAIN_AUDIENCE=%s\n' "$KEYCLOAK_CLIENT_ID"
      printf 'OIDC_MAIN_DISCOVERY_URL=%s\n' "$KEYCLOAK_DISCOVERY_URL"
    fi
    if [[ -n "$onboarding_secret" ]]; then
      printf 'ONBOARDING_SECRET=%s\n' "$onboarding_secret"
    fi

    if [[ -n "$max_concurrent_uni_streams" ]]; then
      printf 'MAX_CONCURRENT_UNI_STREAMS=%s\n' "$max_concurrent_uni_streams"
    fi
    if [[ -n "$max_concurrent_bidi_streams" ]]; then
      printf 'MAX_CONCURRENT_BIDI_STREAMS=%s\n' "$max_concurrent_bidi_streams"
    fi
  } >"$node_dir/.env"
}

generate_test_token() {
  local node_dir=$1
  local bootstrap_secret=$2
  local raw_output

  raw_output="$(
    cd "$node_dir"
    env -i PATH="$PATH" "$ARUNA_DOCTOR_BIN" create-token \
      --oidc-username "$KEYCLOAK_OIDC_USERNAME" \
      --oidc-password "$KEYCLOAK_OIDC_PASSWORD" \
      --bootstrap-secret "$bootstrap_secret"
  )"

  raw_output="${raw_output%$'\n'}"
  local token="${raw_output##*$'\n'}"
  [[ -n "$token" ]] || {
    printf 'unexpected token output:\n%s\n' "$raw_output" >&2
    return 1
  }

  printf '%s\n' "$token"
}

extract_onboarding_secret_from_log() {
  local log_file=$1
  local line
  local secret=""

  [[ -f "$log_file" ]] || return 0

  while IFS= read -r line; do
    case "$line" in
      *onboarding_secret=*)
        secret="${line#*onboarding_secret=}"
        secret="${secret%%[[:space:]]*}"
        ;;
    esac
  done <"$log_file"

  printf '%s\n' "$secret"
}

wait_for_initial_onboarding_secret() {
  local log_file=$1
  local pid=$2
  local deadline=$((SECONDS + READY_TIMEOUT_SECS))
  local secret

  while true; do
    secret="$(extract_onboarding_secret_from_log "$log_file")"
    if [[ -n "$secret" ]]; then
      printf '%s\n' "$secret"
      return 0
    fi

    if ! kill -0 "$pid" >/dev/null 2>&1; then
      die "node-1 exited before it logged the initial onboarding secret; inspect $log_file"
    fi
    if ((SECONDS >= deadline)); then
      die "timed out waiting for the initial onboarding secret in $log_file"
    fi
    sleep 1
  done
}

wait_for_http() {
  local name=$1
  local base_url=$2
  local pid=$3
  local deadline=$((SECONDS + READY_TIMEOUT_SECS))

  until curl --silent --fail --output /dev/null "$base_url/swagger-ui"
  do
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      die "$name exited before it became ready; inspect $DEPLOY_ROOT/$name/$name.log"
    fi
    if ((SECONDS >= deadline)); then
      die "timed out waiting for $name at $base_url"
    fi
    sleep 1
  done
}

wait_for_keycloak() {
  local discovery_url=$1
  local deadline=$((SECONDS + READY_TIMEOUT_SECS))

  until curl --silent --fail --output /dev/null "$discovery_url"
  do
    if ((SECONDS >= deadline)); then
      die "timed out waiting for Keycloak at $discovery_url"
    fi
    sleep 1
  done
}

start_keycloak() {
  log "Starting Keycloak with realm import"
  ARUNA_TEST_DEPLOY_KEYCLOAK_PORT="$KEYCLOAK_HTTP_PORT" docker compose \
    --project-name "$KEYCLOAK_PROJECT_NAME" \
    --file "$ROOT_DIR/scripts/keycloak/docker-compose.yml" \
    up --detach

  KEYCLOAK_BASE_URL="http://127.0.0.1:$KEYCLOAK_HTTP_PORT"
  KEYCLOAK_ISSUER="$KEYCLOAK_BASE_URL/realms/$KEYCLOAK_REALM"
  KEYCLOAK_DISCOVERY_URL="$KEYCLOAK_ISSUER/.well-known/openid-configuration"
  wait_for_keycloak "$KEYCLOAK_DISCOVERY_URL"
}

create_onboarding_secret() {
  local base_url=$1
  local token=$2
  local mode=$3
  local response

  response="$(
    curl \
      --silent \
      --show-error \
      --fail \
      --header "Authorization: Bearer $token" \
      --header "Content-Type: application/json" \
      --data "{\"seed_url\":\"$base_url\",\"mode\":\"$mode\",\"expires_in_seconds\":600}" \
      "$base_url/api/v1/admin/onboarding/secrets"
  )"

  json_string_field "$response" "onboarding_secret"
}

create_server_onboarding_secret() {
  create_onboarding_secret "$NODE_1_BASE_URL" "$INITIAL_ADMIN_TOKEN" "Server"
}

onboard_server_node() {
  local name=$1
  local node_dir=$2
  local http_port=$3
  local p2p_port=$4
  local s3_port=$5
  local base_url=$6
  local secret

  secret="$(create_server_onboarding_secret)"
  write_node_env "$node_dir" "$http_port" "$p2p_port" "$s3_port" "$secret"
  start_node "$name" "$node_dir"
  wait_for_http "$name" "$base_url" "$STARTED_PID"
}

start_node() {
  local name=$1
  local node_dir=$2
  local log_file="$node_dir/$name.log"

  (
    cd "$node_dir"
    if [[ -n "${RUST_LOG:-}" ]]; then
      exec env -i PATH="$PATH" RUST_LOG="$RUST_LOG" NO_COLOR=1 CLICOLOR=0 "$ARUNA_BIN"
    else
      exec env -i PATH="$PATH" NO_COLOR=1 CLICOLOR=0 "$ARUNA_BIN"
    fi
  ) >"$log_file" 2>&1 &

  local pid=$!
  PIDS+=("$pid")
  STARTED_PID="$pid"
  printf '%s\n' "$pid" >"$node_dir/$name.pid"
  log "Started $name (pid $pid)"
}

prepare_nodes() {
  local node_index
  local offset
  local node_name
  local node_dir
  local http_port
  local p2p_port
  local s3_port

  for ((node_index = 1; node_index <= NODE_COUNT; node_index++)); do
    offset=$(((node_index - 1) * 10))
    node_name="node-$node_index"
    node_dir="$DEPLOY_ROOT/$node_name"
    http_port=$((BASE_PORT + offset + 1))
    p2p_port=$((BASE_PORT + offset + 2))
    s3_port=$((BASE_PORT + offset + 3))

    mkdir -p "$node_dir"
    NODE_NAMES+=("$node_name")
    NODE_DIRS+=("$node_dir")
    NODE_BASE_URLS+=("http://127.0.0.1:$http_port")
    NODE_HTTP_PORTS+=("$http_port")
    NODE_P2P_PORTS+=("$p2p_port")
    NODE_S3_PORTS+=("$s3_port")
  done
}

assert_node_ports_free() {
  local node_index

  for node_index in "${!NODE_NAMES[@]}"; do
    assert_port_free "${NODE_HTTP_PORTS[$node_index]}"
    assert_port_free "${NODE_P2P_PORTS[$node_index]}"
    assert_port_free "${NODE_S3_PORTS[$node_index]}"
  done
}

write_summary_file() {
  local summary_file=$1
  local node_index

  : >"$summary_file"
  for node_index in "${!NODE_NAMES[@]}"; do
    printf '%s http=%s s3=http://127.0.0.1:%s dir=%s log=%s\n' \
      "${NODE_NAMES[$node_index]}" \
      "${NODE_BASE_URLS[$node_index]}" \
      "${NODE_S3_PORTS[$node_index]}" \
      "${NODE_DIRS[$node_index]}" \
      "${NODE_DIRS[$node_index]}/${NODE_NAMES[$node_index]}.log" \
      >>"$summary_file"
  done

  if [[ "$WITH_KEYCLOAK" == "1" ]]; then
    printf 'keycloak issuer=%s discovery=%s admin=%s/%s\n' \
      "$KEYCLOAK_ISSUER" \
      "$KEYCLOAK_DISCOVERY_URL" \
      "$KEYCLOAK_ADMIN_USER" \
      "$KEYCLOAK_ADMIN_PASSWORD" \
      >>"$summary_file"
  fi
}

print_summary() {
  local summary_file=$1
  local line

  while IFS= read -r line; do
    printf '%s\n' "$line"
  done <"$summary_file"
}

monitor_nodes() {
  local node_index

  while true; do
    for node_index in "${!PIDS[@]}"; do
      if ! kill -0 "${PIDS[$node_index]}" >/dev/null 2>&1; then
        die "${NODE_NAMES[$node_index]} exited unexpectedly; inspect $DEPLOY_ROOT/${NODE_NAMES[$node_index]}/${NODE_NAMES[$node_index]}.log"
      fi
    done
    sleep 2
  done
}

while (($# > 0)); do
  case "$1" in
    --with-keycloak)
      WITH_KEYCLOAK=1
      ;;
    --node-count)
      shift
      [[ $# -gt 0 ]] || die "missing value for --node-count"
      NODE_COUNT=$1
      ;;
    --node-count=*)
      NODE_COUNT="${1#*=}"
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
  shift
done

[[ "$NODE_COUNT" =~ ^[1-9][0-9]*$ ]] || die "--node-count must be a positive integer"

if [[ -z "$KEYCLOAK_HTTP_PORT" ]]; then
  KEYCLOAK_HTTP_PORT=$((BASE_PORT + NODE_COUNT * 10 + 1))
fi

trap cleanup EXIT
trap handle_signal INT TERM

require_command cargo
require_command curl
require_command ss

if [[ "$WITH_KEYCLOAK" == "1" ]]; then
  require_command docker
fi

mkdir -p "$ROOT_DIR/target"
rm -rf "$DEPLOY_ROOT"
mkdir -p "$DEPLOY_ROOT"

prepare_nodes

assert_node_ports_free
if [[ "$WITH_KEYCLOAK" == "1" ]]; then
  assert_port_free "$KEYCLOAK_HTTP_PORT"
fi

log "Building the full release workspace"
cargo build --workspace --release --locked

[[ -x "$ARUNA_BIN" ]] || die "missing binary: $ARUNA_BIN"
[[ -x "$ARUNA_DOCTOR_BIN" ]] || die "missing binary: $ARUNA_DOCTOR_BIN"

NODE_1_BASE_URL="${NODE_BASE_URLS[0]}"

if [[ "$WITH_KEYCLOAK" == "1" ]]; then
  start_keycloak
fi

write_node_env "${NODE_DIRS[0]}" "${NODE_HTTP_PORTS[0]}" "${NODE_P2P_PORTS[0]}" "${NODE_S3_PORTS[0]}"

start_node "${NODE_NAMES[0]}" "${NODE_DIRS[0]}"
NODE_1_PID="$STARTED_PID"
wait_for_http "${NODE_NAMES[0]}" "$NODE_1_BASE_URL" "$NODE_1_PID"

log "Reading the initial onboarding secret from ${NODE_NAMES[0]}"
INITIAL_LOCAL_ONBOARDING_SECRET="$(wait_for_initial_onboarding_secret "${NODE_DIRS[0]}/${NODE_NAMES[0]}.log" "$NODE_1_PID")"

log "Generating the bootstrap admin token from ${NODE_NAMES[0]}"
INITIAL_ADMIN_TOKEN="$(generate_test_token "${NODE_DIRS[0]}" "$INITIAL_LOCAL_ONBOARDING_SECRET")"
printf 'ADMIN_TOKEN=%s\n' "$INITIAL_ADMIN_TOKEN"

for node_index in "${!NODE_NAMES[@]}"; do
  if [[ $node_index -eq 0 ]]; then
    continue
  fi

  log "Onboarding ${NODE_NAMES[$node_index]}"
  onboard_server_node \
    "${NODE_NAMES[$node_index]}" \
    "${NODE_DIRS[$node_index]}" \
    "${NODE_HTTP_PORTS[$node_index]}" \
    "${NODE_P2P_PORTS[$node_index]}" \
    "${NODE_S3_PORTS[$node_index]}" \
    "${NODE_BASE_URLS[$node_index]}"
done

write_summary_file "$DEPLOY_ROOT/summary.txt"

if [[ "$WITH_KEYCLOAK" == "1" ]]; then
  log "$NODE_COUNT aruna nodes and Keycloak are up"
else
  log "$NODE_COUNT aruna nodes are up"
fi

log "Deployment summary:"
print_summary "$DEPLOY_ROOT/summary.txt"

if [[ "$EXIT_AFTER_READY" == "1" ]]; then
  log "Exiting after readiness because ARUNA_TEST_DEPLOY_EXIT_AFTER_READY=1"
  exit 0
fi

log "Press Ctrl-C to stop the deployment"
monitor_nodes
