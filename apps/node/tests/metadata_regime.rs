//! Builds the metadata, RO-Crate and Git tests and their shared fixtures into one test binary.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "512"]

#[path = "operations/blob_node/mod.rs"]
mod blob_node;
#[path = "operations/cold_start.rs"]
mod cold_start;
#[path = "operations/convergence/mod.rs"]
mod convergence;
#[path = "operations/create_backpressure.rs"]
mod create_backpressure;
#[path = "operations/create_concurrency.rs"]
mod create_concurrency;
#[path = "operations/export_peers.rs"]
mod export_peers;
mod git_native;
#[path = "operations/harvest_pipeline.rs"]
mod harvest_pipeline;
#[path = "operations/id_authority.rs"]
mod id_authority;
#[path = "operations/job_calls.rs"]
mod job_calls;
#[path = "operations/materialization_recovery.rs"]
mod materialization_recovery;
#[path = "operations/metadata_calls.rs"]
mod metadata_calls;
#[path = "operations/metadata_crud.rs"]
mod metadata_crud;
#[path = "operations/metadata_forwarding.rs"]
mod metadata_forwarding;
#[path = "operations/metadata_replication.rs"]
mod metadata_replication;
mod metadata_routes;
#[path = "operations/metadata_throughput.rs"]
mod metadata_throughput;
#[path = "operations/profile_contract.rs"]
mod profile_contract;
#[path = "operations/query_concurrency.rs"]
mod query_concurrency;
#[path = "operations/release_members.rs"]
mod release_members;
#[path = "operations/rocrate_drivers.rs"]
mod rocrate_drivers;
#[path = "operations/rocrate_remote.rs"]
mod rocrate_remote;
#[path = "operations/search_pagination.rs"]
mod search_pagination;
mod shared;
#[path = "operations/topology/mod.rs"]
mod topology;

use convergence::fresh_ids;
