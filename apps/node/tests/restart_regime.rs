//! Builds the node restart tests over on-disk storage and their fixtures into one test binary.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "512"]

#[path = "operations/blob_node/mod.rs"]
mod blob_node;
#[path = "operations/compression_migration.rs"]
mod compression_migration;
#[path = "operations/convergence/mod.rs"]
mod convergence;
#[path = "operations/job_calls.rs"]
mod job_calls;
#[path = "operations/job_restart.rs"]
mod job_restart;
#[path = "operations/restart_persistence.rs"]
mod restart_persistence;
#[path = "operations/restart_traffic.rs"]
mod restart_traffic;
#[path = "operations/restore_peers.rs"]
mod restore_peers;
#[path = "operations/topology/mod.rs"]
mod topology;

use convergence::fresh_ids;
