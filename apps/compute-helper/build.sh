#!/bin/sh
# Builds the compute helper image that jobs mount inside their container.
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

set -eu

repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
version=$(sed -n 's/^version = "\(.*\)"$/\1/p' "$repo_root/Cargo.toml" | head -n 1)
image=${ARUNA_COMPUTE_HELPER_IMAGE:-ghcr.io/arunaengine/aruna-compute-helper:v$version}

docker build -f "$repo_root/apps/compute-helper/Dockerfile" -t "$image" "$repo_root"
