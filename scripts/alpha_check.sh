#!/usr/bin/env bash
# Installs a published node image into a fresh directory, uses its API and S3, restarts it and
# replaces it with a new container on a copy of its stopped data directory.
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION="$(sed -n 's/^version = "\(.*\)"/\1/p' "$ROOT_DIR/Cargo.toml" | head -n 1)"
IMAGE="${ARUNA_ALPHA_IMAGE:-harbor.computational.bio.uni-giessen.de/aruna/aruna-v3-demo:v$VERSION}"
WORK_DIR=""
KEEP=0
READY_TIMEOUT_SECS="${ARUNA_ALPHA_READY_TIMEOUT_SECS:-180}"
CONTAINERS=()

usage() {
  cat <<'EOF'
Usage: bash scripts/alpha_check.sh [--image IMAGE] [--keep] [--help]

Runs one published node image on this host with Docker (host network, loopback ports only):
  1. fresh install in a new data directory, readiness, admin token from a minted bootstrap secret
  2. API: create a group and an S3 credential; S3: create a bucket, upload and read an object
  3. stop and start the same container; the object and the node identity remain
  4. replacement: stop, copy the stopped data directory, start a new container on the copy;
     the object and the node identity remain

  --image IMAGE  Image to check (default: the published image of the workspace version,
                 or ARUNA_ALPHA_IMAGE).
  --keep         Keep the work directory under target/alpha-check for inspection.

The data directory holds the node identity and keys, so the copy in step 4 is all a replacement
needs. Exits non-zero at the first failed step.
EOF
}

log() {
  printf '==> %s\n' "$*" >&2
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

cleanup() {
  local container
  for container in "${CONTAINERS[@]}"; do
    docker rm -f "$container" >/dev/null 2>&1 || true
  done
  if [[ -n "$WORK_DIR" && "$KEEP" != "1" ]]; then
    rm -rf "$WORK_DIR"
  fi
}

free_port() {
  python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --image)
      IMAGE="${2:?--image needs a value}"
      shift 2
      ;;
    --keep)
      KEEP=1
      shift
      ;;
    --help | -h)
      usage
      exit 0
      ;;
    *) die "unknown argument: $1" ;;
  esac
done

command -v docker >/dev/null || die "docker is required"
command -v curl >/dev/null || die "curl is required"
mkdir -p "$ROOT_DIR/target/alpha-check"
WORK_DIR="$(mktemp -d "$ROOT_DIR/target/alpha-check/run-XXXXXX")"
trap cleanup EXIT
# The image has no shell or coreutils; running as this user keeps the data directory ours.
USER_ID="$(id -u):$(id -g)"
RUN_NAME="aruna-alpha-$(basename "$WORK_DIR" | tr '[:upper:]' '[:lower:]')"

HTTP_PORT="$(free_port)"
P2P_PORT="$(free_port)"
S3_PORT="$(free_port)"
OPS_PORT="$(free_port)"
API="http://127.0.0.1:$HTTP_PORT/api/v1"
S3="http://127.0.0.1:$S3_PORT"

cat >"$WORK_DIR/.env" <<EOF
DATABASE_DIR=/data
SOCKET_ADDRESS=127.0.0.1:$HTTP_PORT
P2P_SOCKET_ADDRESS=127.0.0.1:$P2P_PORT
OPS_SOCKET_ADDRESS=127.0.0.1:$OPS_PORT
API_PUBLIC_URL=http://127.0.0.1:$HTTP_PORT
S3_HOST=127.0.0.1:$S3_PORT
S3_PUBLIC_URL=$S3
S3_ADDRESS=127.0.0.1:$S3_PORT
REALM_DESCRIPTION=Alpha_Check_Realm
FEDERATION_REGISTRY_URL=
RUST_LOG=info
EOF
mkdir -p "$WORK_DIR/a"

# Runs a node container named `$1` on data directory `$2`.
start_node() {
  local name=$1
  local data=$2
  docker run -d --name "$name" --network host --user "$USER_ID" \
    -v "$data:/data" -v "$WORK_DIR/.env:/run/.env:ro" "$IMAGE" >/dev/null
  CONTAINERS+=("$name")
  wait_ready "$name"
}

# Waits until container `$1` answers its readiness probe.
wait_ready() {
  local name=$1
  local deadline=$((SECONDS + READY_TIMEOUT_SECS))
  until curl -fsS -m 6 "http://127.0.0.1:$OPS_PORT/readyz" >/dev/null 2>&1; do
    if [[ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" != "true" ]]; then
      docker logs --tail 50 "$name" >&2 || true
      die "$name exited before it was ready"
    fi
    ((SECONDS < deadline)) || die "$name was not ready after ${READY_TIMEOUT_SECS}s"
    sleep 1
  done
}

stop_node() {
  docker stop -t 30 "$1" >/dev/null
}

# Runs aruna-doctor of the image on the stopped data directory `$1`; prints its last line.
doctor() {
  local data=$1
  shift
  docker run --rm --network host --user "$USER_ID" -v "$data:/data" -v "$WORK_DIR/.env:/run/.env:ro" \
    --entrypoint /run/aruna-doctor "$IMAGE" "$@" | tail -n 1
}

api() {
  curl -fsS -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' "$@"
}

s3() {
  curl -fsS --aws-sigv4 "aws:amz:us-east-1:s3" --user "$ACCESS_KEY:$SECRET_KEY" "$@"
}

json_field() {
  python3 -c 'import json, sys; print(json.load(sys.stdin)[sys.argv[1]])' "$1"
}

node_id() {
  api "$API/system/info" | python3 -c 'import json, sys; print(json.load(sys.stdin)["node"]["peer_id"])'
}

log "Image $IMAGE"
docker image inspect "$IMAGE" >/dev/null 2>&1 || docker pull -q "$IMAGE" >/dev/null

log "1. Fresh install"
start_node "$RUN_NAME-a" "$WORK_DIR/a"
stop_node "$RUN_NAME-a"
SECRET="$(doctor "$WORK_DIR/a" recover-admin)"
TOKEN="$(doctor "$WORK_DIR/a" create-token --oidc-username alpha-admin --oidc-password unused \
  --bootstrap-secret "$SECRET")"
[[ -n "$TOKEN" ]] || die "no admin token"
docker start "$RUN_NAME-a" >/dev/null
wait_ready "$RUN_NAME-a"
NODE_ID="$(node_id)"
log "   node $NODE_ID is ready"

log "2. API and S3"
GROUP_ID="$(api -X POST "$API/access/groups" -d '{"name":"alpha-check"}' | json_field group_id)"
CREDENTIAL="$(api -X POST "$API/access/credentials" -d "{\"group_id\":\"$GROUP_ID\"}")"
ACCESS_KEY="$(json_field access_key_id <<<"$CREDENTIAL")"
SECRET_KEY="$(json_field access_secret <<<"$CREDENTIAL")"
s3 -X PUT "$S3/alpha-check" >/dev/null
printf 'alpha check object\n' >"$WORK_DIR/object.txt"
s3 -X PUT --data-binary "@$WORK_DIR/object.txt" "$S3/alpha-check/object.txt" >/dev/null
check_object() {
  local read
  read="$(s3 "$S3/alpha-check/object.txt")"
  [[ "$read" == "alpha check object" ]] || die "$1: the object reads back as '$read'"
  [[ "$(node_id)" == "$NODE_ID" ]] || die "$1: the node identity changed"
  log "   $1: object and node identity kept"
}
check_object "upload"

log "3. Stop and start"
stop_node "$RUN_NAME-a"
docker start "$RUN_NAME-a" >/dev/null
wait_ready "$RUN_NAME-a"
check_object "restart"

log "4. Replacement on a copy of the stopped data directory"
stop_node "$RUN_NAME-a"
cp -a "$WORK_DIR/a" "$WORK_DIR/b"
docker rm "$RUN_NAME-a" >/dev/null
start_node "$RUN_NAME-b" "$WORK_DIR/b"
check_object "replacement"

log "All steps passed for $IMAGE"
