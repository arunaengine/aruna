//! Builds the operation integration test files and their shared fixtures into one test binary.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "512"]

mod access_replication;
mod audit_topology;
mod auth_validation;
mod blob_node;
mod cold_start;
mod compression_migration;
mod convergence;
mod copy_jobs;
mod create_backpressure;
mod create_concurrency;
mod deletion_peers;
mod device_documents;
mod disjoint_turnover;
mod export_peers;
mod genesis_confirmation;
mod group_replication;
mod harvest_pipeline;
mod id_authority;
mod job_calls;
mod job_restart;
mod lane_jobs;
mod linked_import;
mod manifest_convergence;
mod manifest_protocol;
mod materialization_recovery;
mod metadata_calls;
mod metadata_crud;
mod metadata_forwarding;
mod metadata_replication;
mod metadata_throughput;
mod multipart;
mod nonholder_topology;
mod notification_delivery;
mod notification_peers;
mod output_versioning;
mod placement_determinism;
mod profile_contract;
mod propagation_tail;
mod quarantine_capacity;
mod query_concurrency;
mod read_through;
mod release_members;
mod restart_persistence;
mod restart_traffic;
mod restore_peers;
mod rocrate_drivers;
mod rocrate_remote;
mod run_publish;
mod search_pagination;
mod shard_manifest;
mod storage_check;
mod storage_move;
mod storage_purge;
mod synced_folders;
mod table_jobs;
mod token_revocation;
mod topology;
mod transition_bounds;
mod transition_expansion;
mod transition_pinning;
mod transition_stall;
mod transition_turnover;
mod usage_deltas;
mod usage_replication;
mod user_calls;
mod watch_delivery;
