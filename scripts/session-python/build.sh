#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
image=${ARUNA_SESSION_PYTHON_IMAGE:-ghcr.io/arunaengine/aruna-session-python:0.1.0}

docker build -f "$repo_root/scripts/session-python/Dockerfile" -t "$image" "$repo_root"
