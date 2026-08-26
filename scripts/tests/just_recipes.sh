#!/usr/bin/env bash
# Fast contract test for the seven public Just recipes. It never starts a real
# service: stub binaries and fake prerequisite commands cover every path.
set -uo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/aruna-just-recipes.XXXXXX")"
FAKE_BIN="$WORK_DIR/bin"
MIN_BIN="$WORK_DIR/minbin"
STUB_DIR="$WORK_DIR/stub"
DEPLOY_ROOT="$WORK_DIR/deploy"
COMPOSE_DATA_DIR="$WORK_DIR/compose-data"
PORTAL_FIXTURE="$WORK_DIR/portal fixture"
DOCTOR_LOG="$WORK_DIR/doctor.log"
DOCKER_LOG="$WORK_DIR/docker.log"
# Nothing in this test binds a socket; the stub node exits or sleeps instead.
BASE_PORT=47000
FAILED=0

cleanup() {
  local status=$?

  rm -rf "$WORK_DIR"
  exit "$status"
}

trap cleanup EXIT

pass() {
  printf 'ok   %s\n' "$1"
}

fail() {
  printf 'FAIL %s\n' "$1" >&2
  if (($# > 1)); then
    printf '     %s\n' "$2" >&2
  fi
  FAILED=$((FAILED + 1))
}

check_has() {
  local label=$1
  local haystack=$2
  local needle=$3

  if [[ "$haystack" == *"$needle"* ]]; then
    pass "$label"
  else
    fail "$label" "expected to find: $needle"
  fi
}

check_lacks() {
  local label=$1
  local haystack=$2
  local needle=$3

  if [[ "$haystack" == *"$needle"* ]]; then
    fail "$label" "expected not to find: $needle"
  else
    pass "$label"
  fi
}

check_eq() {
  local label=$1
  local expected=$2
  local actual=$3

  if [[ "$expected" == "$actual" ]]; then
    pass "$label"
  else
    fail "$label" "expected [$expected], got [$actual]"
  fi
}

write_stubs() {
  mkdir -p "$FAKE_BIN" "$STUB_DIR" "$PORTAL_FIXTURE"
  printf '<html>fixture</html>\n' >"$PORTAL_FIXTURE/index.html"

  printf '#!/usr/bin/env bash\nexit 0\n' >"$FAKE_BIN/cargo"
  printf '#!/usr/bin/env bash\nexit 0\n' >"$FAKE_BIN/ss"
  # Readiness never succeeds here, so every wait ends in its bounded failure.
  printf '#!/usr/bin/env bash\nexit 1\n' >"$FAKE_BIN/curl"
  cat >"$FAKE_BIN/docker" <<'STUB'
#!/usr/bin/env bash
printf 'docker %s\n' "$*" >>"$DOCKER_LOG"
if [[ "${1:-}" == "image" && "${FAKE_MISSING_IMAGE:-0}" == "1" ]]; then
  exit 1
fi
exit 0
STUB
  cat >"$STUB_DIR/aruna-doctor" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail
printf 'argv: %s\n' "$*" >>"$DOCTOR_LOG"
printf 'PORTAL_ARTIFACT_URL: %s\n' "${PORTAL_ARTIFACT_URL:-unset}" >>"$DOCTOR_LOG"
if [[ "${1:-}" == "portal" ]]; then
  portal_dir=""
  while (($# > 0)); do
    if [[ "$1" == "--portal-dir" ]]; then
      shift
      portal_dir="${1:-}"
    fi
    shift
  done
  mkdir -p "$portal_dir"
  printf '<html>downloaded</html>\n' >"$portal_dir/index.html"
  exit 0
fi
printf 'test-token\n'
STUB
  printf '#!/usr/bin/env bash\nexit 0\n' >"$STUB_DIR/aruna"
  # exec, so stopping the stub leaves no sleep behind.
  printf '#!/usr/bin/env bash\nexec sleep 300\n' >"$STUB_DIR/aruna-alive"
  # A node the stop script can see and stop: it keeps its own name as comm,
  # which an env shebang would replace with bash, and leaves on SIGTERM.
  printf '#!/bin/bash\ntrap "exit 0" TERM\nwhile :; do sleep 1; done\n' >"$STUB_DIR/aruna-node"
  chmod +x "$FAKE_BIN"/* "$STUB_DIR"/*
}

write_min_path() {
  local command
  local resolved

  mkdir -p "$MIN_BIN"
  for command in bash mkdir rm ln tail awk ip cat sleep dirname basename env; do
    resolved="$(command -v "$command" 2>/dev/null)" || continue
    ln -sf "$resolved" "$MIN_BIN/$command"
  done
}

# Runs the cluster script through Just with stub binaries and a test-owned root.
run_recipe() {
  : >"$DOCTOR_LOG"
  : >"$DOCKER_LOG"
  rm -rf "$DEPLOY_ROOT"

  PATH="$FAKE_BIN:$PATH" \
    DOCTOR_LOG="$DOCTOR_LOG" \
    DOCKER_LOG="$DOCKER_LOG" \
    ARUNA_TEST_DEPLOY_ROOT="${ARUNA_TEST_DEPLOY_ROOT:-$DEPLOY_ROOT}" \
    ARUNA_TEST_DEPLOY_SKIP_BUILD=1 \
    ARUNA_TEST_DEPLOY_ARUNA_BIN="${STUB_NODE_BIN:-$STUB_DIR/aruna}" \
    ARUNA_TEST_DEPLOY_DOCTOR_BIN="$STUB_DIR/aruna-doctor" \
    ARUNA_TEST_DEPLOY_BASE_PORT="${ARUNA_TEST_DEPLOY_BASE_PORT:-$BASE_PORT}" \
    ARUNA_TEST_DEPLOY_READY_TIMEOUT_SECS=1 \
    just "$@" 2>&1
}

run_cluster() {
  PATH="$FAKE_BIN:$PATH" \
    DOCTOR_LOG="$DOCTOR_LOG" \
    DOCKER_LOG="$DOCKER_LOG" \
    ARUNA_TEST_DEPLOY_ROOT="${ARUNA_TEST_DEPLOY_ROOT:-$DEPLOY_ROOT}" \
    ARUNA_TEST_DEPLOY_SKIP_BUILD=1 \
    ARUNA_TEST_DEPLOY_ARUNA_BIN="$STUB_DIR/aruna" \
    ARUNA_TEST_DEPLOY_DOCTOR_BIN="$STUB_DIR/aruna-doctor" \
    ARUNA_TEST_DEPLOY_BASE_PORT="${ARUNA_TEST_DEPLOY_BASE_PORT:-$BASE_PORT}" \
    ARUNA_TEST_DEPLOY_READY_TIMEOUT_SECS=1 \
    bash "$ROOT_DIR/scripts/local_cluster_deploy.sh" "$@" 2>&1
}

run_compose() {
  : >"$DOCKER_LOG"

  PATH="$FAKE_BIN:$PATH" \
    DOCKER_LOG="$DOCKER_LOG" \
    ARUNA_COMPOSE_DATA_DIR="${COMPOSE_DATA_DIR_OVERRIDE:-$COMPOSE_DATA_DIR}" \
    ARUNA_COMPOSE_READY_TIMEOUT_SECS=1 \
    ARUNA_COMPOSE_SKIP_BUILD=1 \
    bash "$ROOT_DIR/scripts/local_deploy.sh" "$@" 2>&1
}

node_dir_count() {
  local dir
  local count=0

  for dir in "$DEPLOY_ROOT"/node-*; do
    [[ -d "$dir" ]] && count=$((count + 1))
  done
  printf '%s\n' "$count"
}

test_recipe_list() {
  local listing
  local names

  listing="$(cd "$ROOT_DIR" && just --list 2>&1)"
  names="$(
    printf '%s\n' "$listing" \
      | awk '/^ +[a-z]/ { print $1 }' \
      | sort \
      | tr '\n' ' '
  )"
  check_eq "recipe list is exactly the seven public recipes" \
    "local local-cluster local-cluster-oidc local-new preview preview-no-oidc stop " "$names"
}

test_dry_runs() {
  local recipe
  local output

  for recipe in local local-new local-cluster local-cluster-oidc preview preview-no-oidc stop; do
    if output="$(cd "$ROOT_DIR" && just --dry-run "$recipe" 2>&1)"; then
      pass "dry run of $recipe"
    else
      fail "dry run of $recipe" "$output"
    fi
  done

  output="$(cd "$ROOT_DIR" && just --dry-run local 2>&1)"
  check_has "local runs the compose script" "$output" "bash scripts/local_deploy.sh"
  output="$(cd "$ROOT_DIR" && just --dry-run local-new 2>&1)"
  check_has "local-new asks for fresh state" "$output" "scripts/local_deploy.sh --new"
  output="$(cd "$ROOT_DIR" && just --dry-run local-cluster 2>&1)"
  check_has "local-cluster defaults to three nodes" "$output" "--node-count 3"
  output="$(cd "$ROOT_DIR" && just --dry-run local-cluster-oidc 2>&1)"
  check_has "local-cluster-oidc starts keycloak" "$output" "--with-keycloak --node-count 3"
  output="$(cd "$ROOT_DIR" && just --dry-run preview 2>&1)"
  check_has "preview enables automatic portal acquisition" "$output" "--auto-portal-dir"
  check_has "preview keeps keycloak" "$output" "--with-keycloak"
  output="$(cd "$ROOT_DIR" && just --dry-run preview-no-oidc 2>&1)"
  check_lacks "preview-no-oidc omits keycloak" "$output" "--with-keycloak"
  output="$(cd "$ROOT_DIR" && just --dry-run stop 2>&1)"
  check_has "stop runs the stop script" "$output" "bash scripts/local_cluster_stop.sh"
}

test_help_text() {
  local output

  output="$(bash "$ROOT_DIR/scripts/local_deploy.sh" --help 2>&1)"
  check_has "compose help names the real script" "$output" "bash scripts/local_deploy.sh"
  check_lacks "compose help drops the obsolete name" "$output" "bootstrap_compose.sh"
  check_has "compose help documents --new" "$output" "--new"
  check_has "compose help documents ops readiness" "$output" "/readyz"

  output="$(bash "$ROOT_DIR/scripts/local_cluster_deploy.sh" --help 2>&1)"
  check_has "cluster help names the real script" "$output" "bash scripts/local_cluster_deploy.sh"
  check_has "cluster help documents --node-count" "$output" "--node-count N"
  check_has "cluster help documents --portal-dir" "$output" "--portal-dir P"
  check_has "cluster help documents --auto-portal-dir" "$output" "--auto-portal-dir"
  check_has "cluster help documents ops readiness" "$output" "/readyz"
  check_has "cluster help documents the deployment root" "$output" "ARUNA_TEST_DEPLOY_ROOT"
  check_has "cluster help names the stop recipe" "$output" "just stop"

  output="$(bash "$ROOT_DIR/scripts/local_cluster_stop.sh" --help 2>&1)"
  check_has "stop help names the real script" "$output" "bash scripts/local_cluster_stop.sh"
  check_has "stop help documents the deployment root" "$output" "ARUNA_TEST_DEPLOY_ROOT"
  output="$(bash "$ROOT_DIR/scripts/local_cluster_stop.sh" --bogus 2>&1 || true)"
  check_has "unknown stop argument is rejected" "$output" "unknown argument: --bogus"
}

test_stop_recipe() {
  # A node named by its pid file is stopped, a foreign pid and a dead one are
  # left alone, and Keycloak is taken down even when no node was running.
  local output
  local node_pid
  local foreign_pid

  rm -rf "$DEPLOY_ROOT"
  mkdir -p "$DEPLOY_ROOT/node-1" "$DEPLOY_ROOT/node-2" "$DEPLOY_ROOT/node-3"
  "$STUB_DIR/aruna-node" &
  node_pid=$!
  sleep 300 &
  foreign_pid=$!
  printf '%s\n' "$node_pid" >"$DEPLOY_ROOT/node-1/node-1.pid"
  printf '%s\n' "$foreign_pid" >"$DEPLOY_ROOT/node-2/node-2.pid"
  printf '%s\n' 2147483000 >"$DEPLOY_ROOT/node-3/node-3.pid"
  : >"$DOCKER_LOG"

  output="$(cd "$ROOT_DIR" && PATH="$FAKE_BIN:$PATH" DOCKER_LOG="$DOCKER_LOG" \
    ARUNA_TEST_DEPLOY_ROOT="$DEPLOY_ROOT" ARUNA_TEST_DEPLOY_STOP_TIMEOUT_SECS=5 just stop 2>&1)"

  if kill -0 "$node_pid" 2>/dev/null; then
    fail "stop ends the node named by its pid file" "$output"
    kill "$node_pid" 2>/dev/null || true
  else
    pass "stop ends the node named by its pid file"
  fi
  check_lacks "stop drops the pid file of a stopped node" "$(ls "$DEPLOY_ROOT/node-1")" "node-1.pid"
  if kill -0 "$foreign_pid" 2>/dev/null; then
    pass "stop leaves a reused pid alone"
  else
    fail "stop leaves a reused pid alone" "$output"
  fi
  kill "$foreign_pid" 2>/dev/null || true
  check_has "stop names a reused pid" "$output" "not to a node"
  check_has "stop reports a dead node" "$output" "already gone"
  check_has "stop takes keycloak down" "$(cat "$DOCKER_LOG")" \
    "compose --project-name aruna-test-deploy-oidc"
  check_has "stop removes the keycloak volumes" "$(cat "$DOCKER_LOG")" "down --volumes"
  wait "$node_pid" 2>/dev/null || true
  wait "$foreign_pid" 2>/dev/null || true
}

test_input_rejection() {
  local output
  local value

  for value in 0 -1 abc; do
    output="$(run_cluster --node-count "$value")"
    check_has "node count $value is rejected" "$output" "--node-count must be a positive integer"
  done

  output="$(run_cluster --node-count)"
  check_has "missing node count is rejected" "$output" "missing value for --node-count"

  output="$(run_cluster --bogus)"
  check_has "unknown cluster argument is rejected" "$output" "unknown argument: --bogus"

  output="$(ARUNA_TEST_DEPLOY_BASE_PORT=abc run_cluster --node-count 2)"
  check_has "nonnumeric base port is rejected" "$output" "ARUNA_TEST_DEPLOY_BASE_PORT must be a positive integer"

  output="$(ARUNA_TEST_DEPLOY_BASE_PORT=80 run_cluster --node-count 2)"
  check_has "privileged base port is rejected" "$output" "must be at least 1024"

  output="$(ARUNA_TEST_DEPLOY_READY_ATTEMPT_TIMEOUT_SECS=3 run_cluster --node-count 2)"
  check_has "short readiness attempt timeout is rejected" "$output" "must be at least 6"

  output="$(run_cluster --portal-dir "$WORK_DIR/absent")"
  check_has "missing portal dist is rejected" "$output" "portal dist directory not found"

  output="$(ARUNA_COMPOSE_READY_ATTEMPT_TIMEOUT_SECS=1 run_compose)"
  check_has "short compose attempt timeout is rejected" "$output" "must be at least 6"

  output="$(ARUNA_COMPOSE_ENV_FILE="$WORK_DIR/absent.env" run_compose)"
  check_has "missing compose env file is rejected" "$output" "compose env file not found"

  output="$(run_compose --bogus)"
  check_has "unknown compose argument is rejected" "$output" "unknown argument: --bogus"
}

test_cleanup_guards() {
  local output
  local target

  for target in "$HOME" / "$ROOT_DIR"; do
    output="$(ARUNA_TEST_DEPLOY_ROOT="$target" run_cluster --node-count 2)"
    check_has "cluster refuses to remove $target" "$output" "refusing to remove"
  done

  output="$(COMPOSE_DATA_DIR_OVERRIDE="$HOME" run_compose --new)"
  check_has "compose refuses to clear the home directory" "$output" "refusing to clear"
  check_eq "compose refuses before any docker call" "" "$(cat "$DOCKER_LOG")"

  output="$(COMPOSE_DATA_DIR_OVERRIDE="$ROOT_DIR" run_compose --new)"
  check_has "compose refuses to clear the repository" "$output" "refusing to clear"
}

test_preflight_paths() {
  local output

  output="$(PATH="$MIN_BIN" ARUNA_TEST_DEPLOY_ROOT="$DEPLOY_ROOT" \
    bash "$ROOT_DIR/scripts/local_cluster_deploy.sh" --node-count 2 2>&1)"
  check_has "missing cargo is reported" "$output" "missing required command: cargo"

  output="$(PATH="$MIN_BIN" ARUNA_TEST_DEPLOY_ROOT="$DEPLOY_ROOT" ARUNA_TEST_DEPLOY_SKIP_BUILD=1 \
    bash "$ROOT_DIR/scripts/local_cluster_deploy.sh" --node-count 2 2>&1)"
  check_has "skip-build still requires curl" "$output" "missing required command: curl"

  output="$(PATH="$MIN_BIN:$FAKE_BIN" ARUNA_TEST_DEPLOY_ROOT="$DEPLOY_ROOT" \
    ARUNA_TEST_DEPLOY_SKIP_BUILD=1 \
    ARUNA_TEST_DEPLOY_ARUNA_BIN="$WORK_DIR/absent-binary" \
    bash "$ROOT_DIR/scripts/local_cluster_deploy.sh" --node-count 2 2>&1)"
  check_has "skip-build verifies the built binaries" "$output" "missing binary:"

  output="$(FAKE_MISSING_IMAGE=1 run_compose)"
  check_has "compose skip-build verifies the image" "$output" "image is missing"
}

test_argument_flow() {
  local output

  output="$(run_recipe local-cluster nodes=2)"
  check_eq "just parameter form reaches the node count" "2" "$(node_dir_count)"
  check_lacks "plain cluster never downloads a portal" "$(cat "$DOCTOR_LOG")" "portal update"

  output="$(run_recipe local-cluster 2)"
  check_eq "positional node count is honored" "2" "$(node_dir_count)"

  output="$(PORTAL_ARTIFACT_URL="http://127.0.0.1:1/portal.tar.gz" run_recipe preview nodes=2)"
  check_eq "preview without a portal dir keeps the node count" "2" "$(node_dir_count)"
  check_has "preview downloads into the deployment root" "$(cat "$DOCTOR_LOG")" \
    "--portal-dir $DEPLOY_ROOT/portal"
  check_has "the artifact override reaches the downloader" "$(cat "$DOCTOR_LOG")" \
    "PORTAL_ARTIFACT_URL: http://127.0.0.1:1/portal.tar.gz"

  output="$(PORTAL_ARTIFACT_URL="http://127.0.0.1:1/portal.tar.gz" \
    run_recipe preview-no-oidc "portal_dir=$PORTAL_FIXTURE" nodes=2)"
  check_eq "explicit portal dir keeps the node count" "2" "$(node_dir_count)"
  check_lacks "explicit portal dir wins over the artifact override" "$(cat "$DOCTOR_LOG")" \
    "portal update"
  check_has "explicit portal dir reaches the node env" "$(cat "$DEPLOY_ROOT/node-1/.env")" \
    "PORTAL_DIR='$PORTAL_FIXTURE'"

  output="$(run_recipe preview-no-oidc "$PORTAL_FIXTURE" 2)"
  check_has "a portal path with spaces survives positionally" "$(cat "$DEPLOY_ROOT/node-1/.env")" \
    "PORTAL_DIR='$PORTAL_FIXTURE'"

  # Just hands named values over by position, so the swapped order arrives as
  # --node-count portal_dir=P --portal-dir nodes=2 and must mean the same.
  output="$(PORTAL_ARTIFACT_URL="http://127.0.0.1:1/portal.tar.gz" \
    run_recipe preview-no-oidc nodes=2 "portal_dir=$PORTAL_FIXTURE")"
  check_eq "swapped named values keep the node count" "2" "$(node_dir_count)"
  check_lacks "swapped named values never download" "$(cat "$DOCTOR_LOG")" "portal update"
  check_has "swapped named values reach the node env" "$(cat "$DEPLOY_ROOT/node-1/.env")" \
    "PORTAL_DIR='$PORTAL_FIXTURE'"
}

test_readiness_contract() {
  local output
  local ops_port=$((BASE_PORT + 4))

  output="$(STUB_NODE_BIN="$STUB_DIR/aruna-alive" run_recipe local-cluster nodes=1)"
  check_has "cluster readiness waits on the ops port" "$output" \
    "http://127.0.0.1:$ops_port/readyz"
  check_lacks "cluster readiness never waits on swagger" "$output" "swagger-ui"

  output="$(run_recipe local-cluster nodes=1)"
  check_has "a dead node fails with its log path" "$output" "exited before it became ready"

  printf 'OPS_SOCKET_ADDRESS=127.0.0.1:13002\nSOCKET_ADDRESS=127.0.0.1:13000\n' \
    >"$WORK_DIR/compose.env"
  output="$(ARUNA_COMPOSE_ENV_FILE="$WORK_DIR/compose.env" run_compose)"
  check_has "compose readiness uses the mounted ops port" "$output" \
    "http://127.0.0.1:13002/readyz"
  check_lacks "compose readiness never waits on swagger" "$output" "swagger-ui"

  output="$(run_compose)"
  check_has "compose readiness defaults to the ops port" "$output" \
    "http://127.0.0.1:3002/readyz"
}

test_static_gates() {
  local output

  for script in "$ROOT_DIR"/scripts/*.sh "$ROOT_DIR/scripts/tests/just_recipes.sh"; do
    if output="$(bash -n "$script" 2>&1)"; then
      pass "bash -n $(basename "$script")"
    else
      fail "bash -n $(basename "$script")" "$output"
    fi
  done

  if command -v shellcheck >/dev/null 2>&1; then
    if output="$(cd "$ROOT_DIR" && shellcheck scripts/*.sh scripts/tests/*.sh 2>&1)"; then
      pass "shellcheck"
    else
      fail "shellcheck" "$output"
    fi
  else
    printf 'skip shellcheck (not installed)\n'
  fi

  if docker compose version >/dev/null 2>&1; then
    if output="$(cd "$ROOT_DIR" \
      && docker compose -f scripts/compose.yaml config --quiet 2>&1 \
      && docker compose -f scripts/keycloak/docker-compose.yml config --quiet 2>&1)"; then
      pass "compose files validate"
    else
      fail "compose files validate" "$output"
    fi
  else
    printf 'skip compose validation (docker compose unavailable)\n'
  fi
}

command -v just >/dev/null 2>&1 || {
  printf 'error: missing required command: just\n' >&2
  exit 1
}

write_stubs
write_min_path
mkdir -p "$COMPOSE_DATA_DIR"

test_recipe_list
test_dry_runs
test_help_text
test_stop_recipe
test_input_rejection
test_cleanup_guards
test_preflight_paths
test_argument_flow
test_readiness_contract
test_static_gates

if ((FAILED > 0)); then
  printf '%s check(s) failed\n' "$FAILED" >&2
  exit 1
fi

printf 'all just recipe contracts hold\n'
