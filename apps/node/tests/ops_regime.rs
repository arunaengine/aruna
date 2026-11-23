//! Groups the CLI, CORS, HTTPS and observability tests under one test regime.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#[path = "cli.rs"]
mod cli;
#[path = "cors.rs"]
mod cors;
#[path = "https.rs"]
mod https;
#[path = "observability.rs"]
mod observability;
