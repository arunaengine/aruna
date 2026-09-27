//! The node library: settings, identity, startup phases, shutdown ordering and telemetry.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "512"]
#![allow(clippy::result_large_err)]

pub mod application;
pub mod bootstrap;
pub mod compute_setup;
pub mod config;
pub mod default_env;
pub mod identity;
pub mod portal;
pub mod settings;
pub mod shutdown;
pub mod startup;
pub mod telemetry;
