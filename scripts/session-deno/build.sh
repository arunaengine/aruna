#!/bin/sh
# Builds the Deno session image used by interactive sessions.
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

set -eu

repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
image=${ARUNA_SESSION_DENO_IMAGE:-harbor.computational.bio.uni-giessen.de/aruna/aruna-session-deno:0.1.0}

docker build -f "$repo_root/scripts/session-deno/Dockerfile" -t "$image" "$repo_root"
