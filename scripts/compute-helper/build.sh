#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
image=${ARUNA_COMPUTE_HELPER_IMAGE:-aruna-compute-helper:local}

docker build -f "$repo_root/scripts/compute-helper/Dockerfile" -t "$image" "$repo_root"
