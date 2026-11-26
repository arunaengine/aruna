#!/usr/bin/env bash
# Builds the node and companion images with GitHub Container Registry names, and pushes them on request.
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION="$(sed -n 's/^version = "\(.*\)"/\1/p' "$ROOT_DIR/Cargo.toml" | head -n 1)"
PREFIX="${ARUNA_IMAGE_PREFIX:-ghcr.io/arunaengine}"
PORTAL_REPO="${ARUNA_PORTAL_REPO:-arunaengine/website}"
PORTAL_FILE="aruna-portal-dist.tar.gz"
SOURCE_URL="https://github.com/arunaengine/aruna"

# Name, Dockerfile and build context. Session images also carry the fixed version that
# domain/core/src/compute/runtimes.rs pulls.
IMAGES=(
  "aruna Dockerfile ."
  "aruna-compute-helper apps/compute-helper/Dockerfile ."
  "aruna-registry apps/registry/Dockerfile ."
  "aruna-session-python scripts/session-python/Dockerfile . 0.2.0"
  "aruna-session-deno scripts/session-deno/Dockerfile . 0.1.0"
  "aruna-coordinator apps/coordinator/Dockerfile ."
  "aruna-applet-builder apps/applet-builder/Dockerfile apps/applet-builder"
)

PUSH=0
LIST=0
PORTAL_TAG=""
DIGESTS=""
TAGS=()
CACHE_FROM=()
SELECTED=()

usage() {
  cat <<'EOF'
Usage: bash scripts/build_images.sh [options] [IMAGE...]

Builds linux/amd64 images named ghcr.io/arunaengine/IMAGE. Without IMAGE it builds every
image whose Dockerfile exists in this checkout: aruna (the node), aruna-compute-helper,
aruna-registry, aruna-session-python, aruna-session-deno, aruna-coordinator and
aruna-applet-builder.

  --push            Push the images. Without it they are loaded into the local Docker.
  --tag TAG         Tag every image with TAG; repeatable (default: v<workspace version>).
                    Session images also get their own fixed version, which a push never
                    overwrites once it exists.
  --portal TAG      Download the portal release TAG of arunaengine/website, check its
                    SHA-256 and unpack it into .portal-embed, replacing what is there. The
                    node image then also gets the tag <first tag>-portal.TAG.
  --cache-from SPEC Pass a Buildx cache source; repeatable.
  --digests FILE    Append one JSON line per built image with its name, tags and digest.
  --list            Print the images whose Dockerfile exists, one per line, and exit.
  --help            Show this help.

A node image build uses binaries in .prebuilt/ when both are present, and otherwise compiles
them. Without --portal it embeds what .portal-embed holds, or no portal when it is absent.
ARUNA_IMAGE_PREFIX replaces ghcr.io/arunaengine.
EOF
}

log() {
  printf '==> %s\n' "$*" >&2
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

# Docker tags and release tags share one safe character set.
check_name() {
  [[ "$1" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]] || die "invalid tag: $1"
}

fetch_portal() {
  local base work
  base="https://github.com/$PORTAL_REPO/releases/download/$PORTAL_TAG"
  work="$(mktemp -d)"
  log "downloading portal $PORTAL_TAG from $PORTAL_REPO"
  curl -fsSL --retry 3 -o "$work/$PORTAL_FILE" "$base/$PORTAL_FILE"
  curl -fsSL --retry 3 -o "$work/$PORTAL_FILE.sha256" "$base/$PORTAL_FILE.sha256"
  (cd "$work" && sha256sum --check --strict "$PORTAL_FILE.sha256") >&2 \
    || die "portal $PORTAL_TAG does not match its SHA-256 file"
  rm -rf "$ROOT_DIR/.portal-embed"
  mkdir -p "$ROOT_DIR/.portal-embed"
  tar -xzf "$work/$PORTAL_FILE" -C "$ROOT_DIR/.portal-embed"
  rm -rf "$work"
}

image_exists() {
  docker buildx imagetools inspect "$1" >/dev/null 2>&1
}

build_image() {
  local name="$1" dockerfile="$2" context="$3" fixed="${4:-}"
  local repo="$PREFIX/$name" meta digest tag
  local tags=("${TAGS[@]}")
  if [[ "$name" == "aruna" && -n "$PORTAL_TAG" ]]; then
    tags+=("${TAGS[0]}-portal.$PORTAL_TAG")
  fi
  if [[ -n "$fixed" ]]; then
    if [[ "$PUSH" == "1" ]] && image_exists "$repo:$fixed"; then
      log "$repo:$fixed exists and stays unchanged"
    else
      tags+=("$fixed")
    fi
  fi

  local args=(buildx build --platform linux/amd64 -f "$ROOT_DIR/$dockerfile"
    --label "org.opencontainers.image.source=$SOURCE_URL")
  for tag in "${tags[@]}"; do
    args+=(-t "$repo:$tag")
  done
  for tag in "${CACHE_FROM[@]}"; do
    args+=(--cache-from "$tag")
  done
  meta="$(mktemp)"
  if [[ "$PUSH" == "1" ]]; then
    args+=(--push)
  else
    args+=(--load)
  fi
  log "building $repo with tags ${tags[*]}"
  docker "${args[@]}" --metadata-file "$meta" "$ROOT_DIR/$context"
  digest="$(jq -r '."containerimage.digest" // empty' "$meta")"
  rm -f "$meta"
  [[ -n "$digest" ]] || die "no digest for $repo"
  log "$repo@$digest"
  if [[ -n "$DIGESTS" ]]; then
    jq -cn --arg name "$name" --arg image "$repo" --arg digest "$digest" \
      '{name: $name, image: $image, digest: $digest, reference: "\($image)@\($digest)",
        tags: $ARGS.positional}' --args "${tags[@]}" >>"$DIGESTS"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --push) PUSH=1 ;;
    --list) LIST=1 ;;
    --tag) [[ $# -ge 2 ]] || die "--tag needs a value"; TAGS+=("$2"); shift ;;
    --portal) [[ $# -ge 2 ]] || die "--portal needs a value"; PORTAL_TAG="$2"; shift ;;
    --cache-from) [[ $# -ge 2 ]] || die "--cache-from needs a value"; CACHE_FROM+=("$2"); shift ;;
    --digests) [[ $# -ge 2 ]] || die "--digests needs a value"; DIGESTS="$2"; shift ;;
    --help | -h) usage; exit 0 ;;
    -*) die "unknown option: $1" ;;
    *) SELECTED+=("$1") ;;
  esac
  shift
done

if [[ "$LIST" == "1" ]]; then
  for entry in "${IMAGES[@]}"; do
    read -r name dockerfile _ <<<"$entry"
    if [[ -f "$ROOT_DIR/$dockerfile" ]]; then
      printf '%s\n' "$name"
    fi
  done
  exit 0
fi

[[ ${#TAGS[@]} -gt 0 ]] || TAGS=("v$VERSION")
for tag in "${TAGS[@]}"; do
  check_name "$tag"
done
if [[ -n "$PORTAL_TAG" ]]; then
  check_name "$PORTAL_TAG"
  check_name "${TAGS[0]}-portal.$PORTAL_TAG"
fi
command -v jq >/dev/null || die "jq is required"
for name in "${SELECTED[@]}"; do
  printf '%s\n' "${IMAGES[@]}" | grep -q "^$name " || die "unknown image: $name"
done

for entry in "${IMAGES[@]}"; do
  read -r name dockerfile context fixed <<<"$entry"
  if [[ ${#SELECTED[@]} -gt 0 ]]; then
    [[ " ${SELECTED[*]} " == *" $name "* ]] || continue
  elif [[ ! -f "$ROOT_DIR/$dockerfile" ]]; then
    continue
  fi
  [[ -f "$ROOT_DIR/$dockerfile" ]] || die "$name has no $dockerfile in this checkout"
  if [[ "$name" == "aruna" && -n "$PORTAL_TAG" ]]; then
    fetch_portal
  fi
  build_image "$name" "$dockerfile" "$context" "$fixed"
done
