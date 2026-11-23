//! Builds the CLI, CORS, HTTPS and observability tests into one test binary.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "512"]

mod cli;
mod cors;
mod https;
mod observability;
mod shared;
