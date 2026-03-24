#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
DEPLOY_ROOT="$ROOT_DIR/target/test-deploy"
ARUNA_BIN="$ROOT_DIR/target/release/aruna"
ARUNA_TOOLS_BIN="$ROOT_DIR/target/release/aruna-tools"
READY_TIMEOUT_SECS="${ARUNA_TEST_DEPLOY_READY_TIMEOUT_SECS:-90}"
EXIT_AFTER_READY="${ARUNA_TEST_DEPLOY_EXIT_AFTER_READY:-0}"
BASE_PORT="${ARUNA_TEST_DEPLOY_BASE_PORT:-43000}"
PIDS=()
STARTED_PID=""

log() {
  printf '==> %s\n' "$*"
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
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

  mkdir -p "$node_dir/storage" "$node_dir/blob"
  {
    printf 'STORAGE_PATH=%s\n' "$node_dir/storage"
    printf 'BLOB_ROOT=%s\n' "$node_dir/blob"
    printf 'SOCKET_ADDRESS=127.0.0.1:%s\n' "$http_port"
    printf 'P2P_SOCKET_ADDRESS=127.0.0.1:%s\n' "$p2p_port"
    printf 'S3_PORT=%s\n' "$s3_port"
    printf 'S3_HOST=localhost\n'
    printf 'S3_ADDRESS=127.0.0.1\n'
    printf 'REALM_DESCRIPTION=Test_Deploy_Realm\n'
    printf 'METADATA_REPLICATION_FACTOR=3\n'
    if [[ -n "$onboarding_secret" ]]; then
      printf 'ONBOARDING_SECRET=%s\n' "$onboarding_secret"
    fi
  } >"$node_dir/.env"
}

generate_test_token() {
  local node_dir=$1
  local raw_output

  raw_output="$(
    cd "$node_dir"
    env -i PATH="$PATH" "$ARUNA_TOOLS_BIN"
  )"

  local token="${raw_output##*TOKEN: }"
  [[ -n "$token" && "$token" != "$raw_output" ]] || {
    printf 'unexpected token output:\n%s\n' "$raw_output" >&2
    return 1
  }

  printf '%s\n' "$token"
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

write_summary() {
  local summary_file=$1
  shift

  : >"$summary_file"
  while (($# > 0)); do
    printf '%s\n' "$1" >>"$summary_file"
    shift
  done
}

monitor_nodes() {
  local names=("node-1" "node-2" "node-3")

  while true; do
    local i=0
    for pid in "${PIDS[@]}"; do
      if ! kill -0 "$pid" >/dev/null 2>&1; then
        die "${names[$i]} exited unexpectedly; inspect $DEPLOY_ROOT/${names[$i]}/${names[$i]}.log"
      fi
      i=$((i + 1))
    done
    sleep 2
  done
}

trap cleanup EXIT
trap handle_signal INT TERM

require_command cargo
require_command curl
require_command ss

mkdir -p "$ROOT_DIR/target"
rm -rf "$DEPLOY_ROOT"
mkdir -p "$DEPLOY_ROOT"

log "Building the full release workspace"
cargo build --workspace --release --locked

[[ -x "$ARUNA_BIN" ]] || die "missing binary: $ARUNA_BIN"
[[ -x "$ARUNA_TOOLS_BIN" ]] || die "missing binary: $ARUNA_TOOLS_BIN"

NODE_1_DIR="$DEPLOY_ROOT/node-1"
NODE_2_DIR="$DEPLOY_ROOT/node-2"
NODE_3_DIR="$DEPLOY_ROOT/node-3"

mkdir -p "$NODE_1_DIR" "$NODE_2_DIR" "$NODE_3_DIR"

NODE_1_HTTP_PORT=$((BASE_PORT + 1))
NODE_1_P2P_PORT=$((BASE_PORT + 2))
NODE_1_S3_PORT=$((BASE_PORT + 3))
NODE_2_HTTP_PORT=$((BASE_PORT + 11))
NODE_2_P2P_PORT=$((BASE_PORT + 12))
NODE_2_S3_PORT=$((BASE_PORT + 13))
NODE_3_HTTP_PORT=$((BASE_PORT + 21))
NODE_3_P2P_PORT=$((BASE_PORT + 22))
NODE_3_S3_PORT=$((BASE_PORT + 23))

for port in \
  "$NODE_1_HTTP_PORT" "$NODE_1_P2P_PORT" "$NODE_1_S3_PORT" \
  "$NODE_2_HTTP_PORT" "$NODE_2_P2P_PORT" "$NODE_2_S3_PORT" \
  "$NODE_3_HTTP_PORT" "$NODE_3_P2P_PORT" "$NODE_3_S3_PORT"
do
  assert_port_free "$port"
done

NODE_1_BASE_URL="http://127.0.0.1:$NODE_1_HTTP_PORT"
NODE_2_BASE_URL="http://127.0.0.1:$NODE_2_HTTP_PORT"
NODE_3_BASE_URL="http://127.0.0.1:$NODE_3_HTTP_PORT"

write_node_env "$NODE_1_DIR" "$NODE_1_HTTP_PORT" "$NODE_1_P2P_PORT" "$NODE_1_S3_PORT"

log "Generating the bootstrap admin token from node-1"
INITIAL_ADMIN_TOKEN="$(generate_test_token "$NODE_1_DIR")"
printf 'ADMIN_TOKEN=%s\n' "$INITIAL_ADMIN_TOKEN"

start_node "node-1" "$NODE_1_DIR"
NODE_1_PID="$STARTED_PID"
wait_for_http "node-1" "$NODE_1_BASE_URL" "$NODE_1_PID"

log "Onboarding node-2"
onboard_server_node "node-2" "$NODE_2_DIR" "$NODE_2_HTTP_PORT" "$NODE_2_P2P_PORT" "$NODE_2_S3_PORT" "$NODE_2_BASE_URL"
NODE_2_PID="$STARTED_PID"

log "Onboarding node-3"
onboard_server_node "node-3" "$NODE_3_DIR" "$NODE_3_HTTP_PORT" "$NODE_3_P2P_PORT" "$NODE_3_S3_PORT" "$NODE_3_BASE_URL"
NODE_3_PID="$STARTED_PID"

write_summary \
  "$DEPLOY_ROOT/summary.txt" \
  "node-1 http=$NODE_1_BASE_URL s3=127.0.0.1:$NODE_1_S3_PORT dir=$NODE_1_DIR log=$NODE_1_DIR/node-1.log" \
  "node-2 http=$NODE_2_BASE_URL s3=127.0.0.1:$NODE_2_S3_PORT dir=$NODE_2_DIR log=$NODE_2_DIR/node-2.log" \
  "node-3 http=$NODE_3_BASE_URL s3=127.0.0.1:$NODE_3_S3_PORT dir=$NODE_3_DIR log=$NODE_3_DIR/node-3.log"

log "Three aruna nodes are up"
log "Deployment summary:"
cat "$DEPLOY_ROOT/summary.txt"

if [[ "$EXIT_AFTER_READY" == "1" ]]; then
  log "Exiting after readiness because ARUNA_TEST_DEPLOY_EXIT_AFTER_READY=1"
  exit 0
fi

log "Press Ctrl-C to stop the deployment"
monitor_nodes
