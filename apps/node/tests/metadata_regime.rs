//! Groups the metadata, RO-Crate and Git tests under one test regime.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#[path = "operations/cold_start.rs"]
mod cold_start;
#[path = "operations/create_backpressure.rs"]
mod create_backpressure;
#[path = "operations/create_concurrency.rs"]
mod create_concurrency;
#[path = "operations/export_peers.rs"]
mod export_peers;
#[path = "git_native.rs"]
mod git_native;
#[path = "operations/harvest_pipeline.rs"]
mod harvest_pipeline;
#[path = "operations/id_authority.rs"]
mod id_authority;
#[path = "operations/materialization_recovery.rs"]
mod materialization_recovery;
#[path = "operations/metadata_crud.rs"]
mod metadata_crud;
#[path = "operations/metadata_forwarding.rs"]
mod metadata_forwarding;
#[path = "operations/metadata_replication.rs"]
mod metadata_replication;
#[path = "metadata_routes.rs"]
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
