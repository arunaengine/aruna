#!/bin/sh
# Enables LLVM source-based coverage for workspace crates without instrumenting third-party deps.
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

set -eu

rustc=$1
shift

instrument=0
prev=""
for arg in "$@"; do
  if [ "$prev" = "--crate-name" ]; then
    case "$arg" in
      build_script_*) exec "$rustc" "$@" ;;
      *) instrument=1 ;;
    esac
  fi
  if [ "$prev" = "--crate-type" ] && [ "$arg" = "proc-macro" ]; then
    exec "$rustc" "$@"
  fi
  prev=$arg
done

if [ "$instrument" -eq 1 ]; then
  exec "$rustc" "$@" -C instrument-coverage
fi
exec "$rustc" "$@"
