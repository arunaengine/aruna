#!/bin/sh
# Builds the compute helper image that jobs mount inside their container.
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

set -eu

repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
image=${ARUNA_COMPUTE_HELPER_IMAGE:-aruna-compute-helper:local}

docker build -f "$repo_root/scripts/compute-helper/Dockerfile" -t "$image" "$repo_root"
