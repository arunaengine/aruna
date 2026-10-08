#!/bin/sh
# Builds the registry image.
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

set -eu

repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
version=$(sed -n 's/^version = "\(.*\)"$/\1/p' "$repo_root/Cargo.toml" | head -n 1)
image=${ARUNA_REGISTRY_IMAGE:-harbor.computational.bio.uni-giessen.de/aruna/aruna-registry:$version}

docker build -f "$repo_root/registry/Dockerfile" -t "$image" "$repo_root"
