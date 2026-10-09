#!/usr/bin/env bash
# Starts or stops two local realms and one registry for the federation check.
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
DEPLOY_ROOT="$(realpath -m -- "${ARUNA_TWO_REALMS_ROOT:-$ROOT_DIR/target/two-realms}")"
PORTAL_DIR="${ARUNA_TEST_DEPLOY_PORTAL_DIR:-}"
A_PORT="${ARUNA_TWO_REALMS_A_PORT:-47100}"
B_PORT="${ARUNA_TWO_REALMS_B_PORT:-47200}"
REGISTRY_PORT="${ARUNA_TWO_REALMS_REGISTRY_PORT:-47301}"
PUBLIC_PORT="${ARUNA_TWO_REALMS_PUBLIC_PORT:-47310}"
REGISTRY_BIN="${ARUNA_TWO_REALMS_REGISTRY_BIN:-$ROOT_DIR/target/release/aruna-registry}"
READY_TIMEOUT_SECS="${ARUNA_TWO_REALMS_READY_TIMEOUT_SECS:-600}"
# Strict egress refuses loopback and private addresses, so the registry and realm A are
# published over TLS on a public test address of a local Docker bridge.
PUBLIC_IP="30.255.255.1"
PUBLIC_SUBNET="30.255.255.0/29"
NETWORK="aruna-two-realms"
# Realm B's public API site needs its own address: one address takes only one CA.
PRIVATE_IP="30.255.255.9"
PRIVATE_SUBNET="30.255.255.8/29"
PRIVATE_NETWORK="aruna-two-realms-private"
PROXY="aruna-two-realms-tls"
SYSTEM_CA="/etc/ssl/certs/ca-certificates.crt"

log() {
  printf '==> %s\n' "$*"
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage: bash scripts/two_realms.sh start | stop

  start  Start realm A (public, 1 node), realm B (private, 2 nodes), each with its own
         Keycloak, and one aruna-registry. Needs release binaries and a built portal in
         ARUNA_TEST_DEPLOY_PORTAL_DIR. Federation settings are left to the real routes.
         Both realms name the local registry as their default registry.
  stop   Stop both realms, their Keycloak projects, the registry and the TLS proxy.

Reachability:
  The registry and realm A are also served over TLS on 30.255.255.1, a public test address
  on a local Docker bridge, because strict egress refuses loopback. Realm A's node publishes
  its API URL there, so realm B can pull from it. Realm B's first node API is served on
  30.255.255.9 under a second CA that only the realms trust, so realm A can push to it when
  realm B's settings name that URL. The registry trusts only the first CA, so it cannot reach B.

Environment overrides:
  ARUNA_TWO_REALMS_ROOT                deployment directory, target/two-realms by default
  ARUNA_TWO_REALMS_A_PORT              base port of realm A, 47100 by default
  ARUNA_TWO_REALMS_B_PORT              base port of realm B, 47200 by default
  ARUNA_TWO_REALMS_REGISTRY_PORT       loopback registry port, 47301 by default
  ARUNA_TWO_REALMS_PUBLIC_PORT         first TLS port on the test address, 47310 by default
  ARUNA_TWO_REALMS_REGISTRY_BIN
  ARUNA_TWO_REALMS_READY_TIMEOUT_SECS  seconds to wait for each realm, 600 by default
  ARUNA_TEST_DEPLOY_PORTAL_DIR         built portal dist directory
EOF
}

# Realm settings for cluster_start.sh and cluster_stop.sh; $1 is a or b.
realm_env() {
  local realm=$1
  local base=$A_PORT
  local nodes=1

  if [[ "$realm" == "b" ]]; then
    base=$B_PORT
    nodes=2
  fi
  printf '%s\n' \
    "ARUNA_TEST_DEPLOY_ROOT=$DEPLOY_ROOT/$realm" \
    "ARUNA_TEST_DEPLOY_BASE_PORT=$base" \
    "ARUNA_TEST_DEPLOY_NODE_COUNT=$nodes" \
    "ARUNA_TEST_DEPLOY_KEYCLOAK_PORT=$((base + nodes * 10 + 1))" \
    "ARUNA_TEST_DEPLOY_KEYCLOAK_PROJECT=aruna-two-realms-$realm"
}

# Loopback portal and API origins of a realm, comma-separated.
realm_origins() {
  local base=$1
  local nodes=$2
  local index
  local origins=()

  for ((index = 0; index < nodes; index++)); do
    origins+=("http://127.0.0.1:$((base + index * 10 + 1))" "http://127.0.0.1:$((base + index * 10 + 5))")
  done
  (IFS=,; printf '%s' "${origins[*]}")
}

write_caddyfile() {
  cat >"$DEPLOY_ROOT/proxy/Caddyfile" <<EOF
{
	admin off
	http_port $((PUBLIC_PORT + 9))
	auto_https disable_redirects
	skip_install_trust
	pki {
		ca private {
			name "Aruna two realms private"
		}
	}
}
https://$PUBLIC_IP:$((PUBLIC_PORT + 1)) {
	bind $PUBLIC_IP
	tls internal
	reverse_proxy 127.0.0.1:$REGISTRY_PORT
}
https://$PUBLIC_IP:$((PUBLIC_PORT + 2)) {
	bind $PUBLIC_IP
	tls internal
	reverse_proxy 127.0.0.1:$((A_PORT + 1))
}
https://$PUBLIC_IP:$((PUBLIC_PORT + 3)) {
	bind $PUBLIC_IP
	tls internal
	reverse_proxy 127.0.0.1:$((A_PORT + 5))
}
https://$PRIVATE_IP:$((PUBLIC_PORT + 4)) {
	bind $PRIVATE_IP
	tls {
		issuer internal {
			ca private
		}
	}
	reverse_proxy 127.0.0.1:$((B_PORT + 1))
}
EOF
}

start_proxy() {
  local root_ca="$DEPLOY_ROOT/proxy/data/caddy/pki/authorities/local/root.crt"
  local private_ca="$DEPLOY_ROOT/proxy/data/caddy/pki/authorities/private/root.crt"
  local deadline=$((SECONDS + 60))

  docker network inspect "$NETWORK" >/dev/null 2>&1 \
    || docker network create --subnet "$PUBLIC_SUBNET" --gateway "$PUBLIC_IP" "$NETWORK" >/dev/null
  docker network inspect "$PRIVATE_NETWORK" >/dev/null 2>&1 || docker network create \
    --subnet "$PRIVATE_SUBNET" --gateway "$PRIVATE_IP" "$PRIVATE_NETWORK" >/dev/null
  mkdir -p "$DEPLOY_ROOT/proxy/data" "$DEPLOY_ROOT/proxy/config"
  write_caddyfile
  docker run --detach --name "$PROXY" --network host --user "$(id -u):$(id -g)" \
    --volume "$DEPLOY_ROOT/proxy/data:/data" --volume "$DEPLOY_ROOT/proxy/config:/config" \
    --volume "$DEPLOY_ROOT/proxy/Caddyfile:/etc/caddy/Caddyfile:ro" caddy:2 >/dev/null
  until [[ -s "$root_ca" && -s "$private_ca" ]]; do
    ((SECONDS < deadline)) || die "the TLS proxy wrote no CA certificate; see docker logs $PROXY"
    sleep 1
  done
  cat "$SYSTEM_CA" "$root_ca" >"$DEPLOY_ROOT/ca-registry.pem"
  cat "$SYSTEM_CA" "$root_ca" "$private_ca" >"$DEPLOY_ROOT/ca-bundle.pem"
}

start_registry() {
  mkdir -p "$DEPLOY_ROOT/registry"
  (
    cd "$DEPLOY_ROOT/registry"
    exec setsid env -i PATH="$PATH" RUST_LOG="${RUST_LOG:-info}" \
      SSL_CERT_FILE="$DEPLOY_ROOT/ca-registry.pem" \
      ARUNA_REGISTRY_LISTEN="127.0.0.1:$REGISTRY_PORT" \
      ARUNA_REGISTRY_DATA="$DEPLOY_ROOT/registry/data" "$REGISTRY_BIN"
  ) >"$DEPLOY_ROOT/registry/registry.log" 2>&1 &
  printf '%s\n' "$!" >"$DEPLOY_ROOT/registry/registry.pid"
}

# Runs cluster_start.sh for one realm in its own session; it keeps monitoring the nodes.
start_realm() {
  local realm=$1
  local nodes=$2
  local extra_origins=$3
  local api_urls=${4:-}

  (
    export ARUNA_TEST_DEPLOY_SKIP_BUILD=1 ARUNA_TEST_DEPLOY_PORTAL_DIR="$PORTAL_DIR"
    export ARUNA_TEST_DEPLOY_EXTRA_ORIGINS="$extra_origins"
    export ARUNA_TEST_DEPLOY_CA_FILE="$DEPLOY_ROOT/ca-bundle.pem"
    export ARUNA_TEST_DEPLOY_API_PUBLIC_URLS="$api_urls"
    export ARUNA_TEST_DEPLOY_REGISTRY_URL="https://$PUBLIC_IP:$((PUBLIC_PORT + 1))"
    while IFS= read -r setting; do
      export "${setting?}"
    done < <(realm_env "$realm")
    exec setsid bash "$ROOT_DIR/scripts/cluster_start.sh" --with-keycloak --node-count "$nodes"
  ) >"$DEPLOY_ROOT/realm-$realm.log" 2>&1 &
  printf '%s\n' "$!" >"$DEPLOY_ROOT/realm-$realm.pid"
}

wait_realm() {
  local realm=$1
  local pid
  local deadline=$((SECONDS + READY_TIMEOUT_SECS))

  pid="$(<"$DEPLOY_ROOT/realm-$realm.pid")"
  until grep -q 'Press Ctrl-C' "$DEPLOY_ROOT/realm-$realm.log" 2>/dev/null; do
    kill -0 "$pid" 2>/dev/null || die "realm $realm failed; see $DEPLOY_ROOT/realm-$realm.log"
    ((SECONDS < deadline)) || die "realm $realm is not ready; see $DEPLOY_ROOT/realm-$realm.log"
    sleep 2
  done
}

write_summary() {
  {
    printf 'Realm A (public)\n'
    printf '  %-22s %s\n' "Public API URL" "https://$PUBLIC_IP:$((PUBLIC_PORT + 2))/api/v1"
    printf '  %-22s %s\n' "Public portal URL" "https://$PUBLIC_IP:$((PUBLIC_PORT + 3))/"
    printf '  %-22s %s\n' "Deployment" "$DEPLOY_ROOT/a/summary.txt"
    printf 'Realm B (private)\n'
    printf '  %-22s %s\n' "API URL" "http://127.0.0.1:$((B_PORT + 1))/api/v1"
    printf '  %-22s %s\n' "Portal URL" "http://127.0.0.1:$((B_PORT + 5))/"
    printf '  %-22s %s\n' "Public API URL" "https://$PRIVATE_IP:$((PUBLIC_PORT + 4))/api/v1"
    printf '  %-22s %s\n' "Deployment" "$DEPLOY_ROOT/b/summary.txt"
    printf 'Registry\n'
    printf '  %-22s %s\n' "URL for realms" "https://$PUBLIC_IP:$((PUBLIC_PORT + 1))"
    printf '  %-22s %s\n' "Loopback URL" "http://127.0.0.1:$REGISTRY_PORT"
    printf '  %-22s %s\n' "CA bundle" "$DEPLOY_ROOT/ca-bundle.pem"
  } >"$DEPLOY_ROOT/summary.txt"
  cat "$DEPLOY_ROOT/summary.txt"
}

assert_free() {
  local port

  for port in "$@"; do
    [[ -z "$(ss -ltnH "sport = :$port")" ]] || die "port $port is already in use"
  done
}

start() {
  local public_origins="https://$PUBLIC_IP:$((PUBLIC_PORT + 2)),https://$PUBLIC_IP:$((PUBLIC_PORT + 3))"
  public_origins+=",https://$PRIVATE_IP:$((PUBLIC_PORT + 4))"

  [[ -x "$REGISTRY_BIN" ]] || die "missing binary: $REGISTRY_BIN"
  [[ -f "$PORTAL_DIR/index.html" ]] || die "set ARUNA_TEST_DEPLOY_PORTAL_DIR to a built portal"
  ! docker container inspect "$PROXY" >/dev/null 2>&1 || die "already running; run stop first"
  [[ "$DEPLOY_ROOT" == /*/*/* && "$ROOT_DIR/" != "$DEPLOY_ROOT"/* ]] \
    || die "refusing to use $DEPLOY_ROOT as the deployment directory"
  assert_free "$REGISTRY_PORT" "$((PUBLIC_PORT + 1))" "$((PUBLIC_PORT + 2))" "$((PUBLIC_PORT + 3))" \
    "$((PUBLIC_PORT + 4))"
  rm -rf "$DEPLOY_ROOT"
  mkdir -p "$DEPLOY_ROOT"
  # A failed or interrupted start stops everything it started.
  trap stop EXIT
  trap 'exit 1' INT TERM HUP
  log "Starting the TLS proxy on $PUBLIC_IP"
  start_proxy
  log "Starting the registry"
  start_registry
  log "Starting realm A and realm B"
  start_realm a 1 "$(realm_origins "$B_PORT" 2),$public_origins" \
    "https://$PUBLIC_IP:$((PUBLIC_PORT + 2))"
  start_realm b 2 "$(realm_origins "$A_PORT" 1),$public_origins"
  wait_realm a
  wait_realm b
  trap - EXIT INT TERM HUP
  log "Both realms and the registry are up"
  write_summary
}

# Whether a recorded supervisor still runs: cluster_start.sh, or this script's own child before
# it starts that script. An exited child or a pid now used by another process does not.
supervisor_alive() {
  local state ppid args

  read -r state ppid args < <(ps -o stat=,ppid=,args= -p "$1" 2>/dev/null || true) || return 1
  [[ "$state" != Z* ]] && [[ "$ppid" == "$$" || "$args" == *scripts/cluster_start.sh* ]]
}

# Stops the deploy script recorded for a realm and waits until its own cleanup has finished. It
# may not have written the pid file cluster_stop.sh uses. It ignores SIGINT as a background job.
stop_supervisor() {
  local pid_file="$DEPLOY_ROOT/realm-$1.pid"
  local pid
  local deadline=$((SECONDS + 120))

  [[ -f "$pid_file" ]] || return 0
  pid="$(<"$pid_file")"
  if supervisor_alive "$pid"; then
    kill -TERM "$pid" 2>/dev/null || true
    while supervisor_alive "$pid"; do
      ((SECONDS < deadline)) || kill -KILL "$pid" 2>/dev/null || true
      sleep 0.2
    done
    log "Stopped the deploy script of realm $1 (pid $pid)"
  fi
  rm -f "$pid_file"
}

stop() {
  local realm
  local pid

  for realm in a b; do
    stop_supervisor "$realm"
    (
      while IFS= read -r setting; do
        export "${setting?}"
      done < <(realm_env "$realm")
      bash "$ROOT_DIR/scripts/cluster_stop.sh"
    )
  done
  if [[ -f "$DEPLOY_ROOT/registry/registry.pid" ]]; then
    pid="$(<"$DEPLOY_ROOT/registry/registry.pid")"
    if [[ "$(ps -o comm= -p "$pid" 2>/dev/null || true)" == aruna-registry* ]]; then
      kill -INT "$pid"
      log "Stopped the registry (pid $pid)"
    fi
    rm -f "$DEPLOY_ROOT/registry/registry.pid"
  fi
  docker rm --force "$PROXY" >/dev/null 2>&1 || true
  docker network rm "$NETWORK" "$PRIVATE_NETWORK" >/dev/null 2>&1 || true
  log "Stopped the TLS proxy and removed its network"
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  --help | -h) usage ;;
  *)
    usage >&2
    exit 1
    ;;
esac
