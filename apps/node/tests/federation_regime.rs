//! Builds the multi-node replication and delivery tests and their fixtures into one test binary.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "512"]

#[path = "operations/audit_topology.rs"]
mod audit_topology;
#[path = "operations/convergence/mod.rs"]
mod convergence;
#[path = "operations/deletion_peers.rs"]
mod deletion_peers;
#[path = "operations/device_documents.rs"]
mod device_documents;
#[path = "operations/disjoint_turnover.rs"]
mod disjoint_turnover;
#[path = "operations/genesis_confirmation.rs"]
mod genesis_confirmation;
#[path = "operations/job_calls.rs"]
mod job_calls;
#[path = "operations/manifest_convergence.rs"]
mod manifest_convergence;
#[path = "operations/manifest_protocol.rs"]
mod manifest_protocol;
#[path = "operations/metadata_calls.rs"]
mod metadata_calls;
mod network_recovery;
#[path = "operations/nonholder_topology.rs"]
mod nonholder_topology;
#[path = "operations/notification_delivery.rs"]
mod notification_delivery;
#[path = "operations/notification_peers.rs"]
mod notification_peers;
#[path = "operations/placement_determinism.rs"]
mod placement_determinism;
#[path = "operations/propagation_tail.rs"]
mod propagation_tail;
#[path = "operations/quarantine_capacity.rs"]
mod quarantine_capacity;
#[path = "operations/read_through.rs"]
mod read_through;
mod replication;
#[path = "operations/shard_manifest.rs"]
mod shard_manifest;
mod shared;
#[path = "operations/synced_folders.rs"]
mod synced_folders;
#[path = "operations/topology/mod.rs"]
mod topology;
#[path = "operations/transition_bounds.rs"]
mod transition_bounds;
#[path = "operations/transition_expansion.rs"]
mod transition_expansion;
#[path = "operations/transition_pinning.rs"]
mod transition_pinning;
#[path = "operations/transition_stall.rs"]
mod transition_stall;
#[path = "operations/transition_turnover.rs"]
mod transition_turnover;
#[path = "operations/usage_replication.rs"]
mod usage_replication;
#[path = "operations/user_calls.rs"]
mod user_calls;
#[path = "operations/watch_delivery.rs"]
mod watch_delivery;

use convergence::fresh_ids;
