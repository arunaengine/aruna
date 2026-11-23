//! Groups the node restart tests over on-disk storage under one test regime.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#[path = "operations/compression_migration.rs"]
mod compression_migration;
#[path = "operations/job_restart.rs"]
mod job_restart;
#[path = "operations/restart_persistence.rs"]
mod restart_persistence;
#[path = "operations/restart_traffic.rs"]
mod restart_traffic;
#[path = "operations/restore_peers.rs"]
mod restore_peers;
