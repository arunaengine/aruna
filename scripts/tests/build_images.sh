#!/usr/bin/env bash
# Contract test for scripts/build_images.sh using a stub docker binary.
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

set -uo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT="$ROOT_DIR/scripts/build_images.sh"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/aruna-build-images.XXXXXX")"
FAKE_BIN="$WORK_DIR/bin"
DOCKER_LOG="$WORK_DIR/docker.log"
DIGESTS="$WORK_DIR/digests.jsonl"
FAILED=0

cleanup() {
  local status=$?
  rm -rf "$WORK_DIR"
  exit "$status"
}

trap cleanup EXIT

mkdir -p "$FAKE_BIN"
cat >"$FAKE_BIN/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >>"$DOCKER_LOG"

if [[ "${1:-}" == "buildx" && "${2:-}" == "imagetools" && "${3:-}" == "inspect" ]]; then
  ref="${4:-}"
  case "${STUB_INSPECT:-missing}" in
    exists) exit 0 ;;
    missing)
      printf 'ERROR: %s: not found\n' "$ref" >&2
      exit 1
      ;;
    manifest_unknown)
      printf 'ERROR: MANIFEST_UNKNOWN: manifest unknown\n' >&2
      exit 1
      ;;
    error)
      printf 'ERROR: failed to do request: 503 Service Unavailable\n' >&2
      exit 1
      ;;
  esac
fi

if [[ "${1:-}" == "buildx" && "${2:-}" == "build" ]]; then
  meta=""
  while [[ $# -gt 0 ]]; do
    if [[ "$1" == "--metadata-file" ]]; then
      meta="$2"
      shift 2
      continue
    fi
    shift
  done
  if [[ -n "$meta" ]]; then
    printf '{"containerimage.digest":"sha256:1111111111111111111111111111111111111111111111111111111111111111"}\n' >"$meta"
  fi
  exit 0
fi

printf 'unexpected docker args: %s\n' "$*" >&2
exit 1
EOF
chmod +x "$FAKE_BIN/docker"

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

reset_logs() {
  : >"$DOCKER_LOG"
  : >"$DIGESTS"
}

# Case 1: non-release --push must not claim fixed session tags.
reset_logs
out="$(PATH="$FAKE_BIN:$PATH" DOCKER_LOG="$DOCKER_LOG" STUB_INSPECT=missing \
  bash "$SCRIPT" --push --tag sha-1234567 --digests "$DIGESTS" aruna-session-python 2>&1)"
rc=$?
if [[ $rc -ne 0 ]]; then
  fail "non-release push exits cleanly" "$out"
elif grep -q 'aruna-session-python:0.2.0' "$DOCKER_LOG" || grep -q '"0.2.0"' "$DIGESTS"; then
  fail "non-release push omits fixed session tag" "$(cat "$DOCKER_LOG")"
else
  pass "non-release push omits fixed session tag"
fi

# Case 2: release --push with --fixed-tags publishes fixed tag when missing.
reset_logs
out="$(PATH="$FAKE_BIN:$PATH" DOCKER_LOG="$DOCKER_LOG" STUB_INSPECT=missing \
  bash "$SCRIPT" --push --fixed-tags --tag v3.0.0-alpha.210 --tag sha-1234567 \
  --digests "$DIGESTS" aruna-session-python 2>&1)"
rc=$?
if [[ $rc -ne 0 ]]; then
  fail "release push with --fixed-tags succeeds" "$out"
elif ! grep -q 'aruna-session-python:0.2.0' "$DOCKER_LOG"; then
  fail "release push with --fixed-tags includes 0.2.0" "$(cat "$DOCKER_LOG")"
else
  pass "release push with --fixed-tags includes 0.2.0"
fi

# Case 3: local --load keeps the fixed session tag without remote inspect.
reset_logs
out="$(PATH="$FAKE_BIN:$PATH" DOCKER_LOG="$DOCKER_LOG" STUB_INSPECT=error \
  bash "$SCRIPT" --tag v3.0.0-alpha.210 aruna-session-python 2>&1)"
rc=$?
if [[ $rc -ne 0 ]]; then
  fail "local load succeeds without registry access" "$out"
elif ! grep -q 'aruna-session-python:0.2.0' "$DOCKER_LOG"; then
  fail "local load includes fixed session tag" "$(cat "$DOCKER_LOG")"
else
  pass "local load includes fixed session tag"
fi

if ((FAILED > 0)); then
  printf '%d test(s) failed\n' "$FAILED" >&2
  exit 1
fi
