//! The node library: startup phases, shutdown ordering, background tasks and telemetry.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "512"]
#![allow(clippy::result_large_err)]

pub mod applets;
pub mod application;
pub mod automations;
pub mod backup;
pub mod bootstrap;
pub mod compute_setup;
pub mod default_env;
pub mod inbound;
mod inbound_frames;
mod peer_frames;
pub mod plugin_host;
pub mod portal;
pub mod shutdown;
pub mod startup;
pub mod tasks;
pub mod telemetry;
