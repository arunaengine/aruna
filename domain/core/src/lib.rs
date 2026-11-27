//! Owns the shared domain types, effects, events, keyspaces and the operation contract.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#![allow(clippy::result_large_err)]

mod access;
mod catalog;
mod network;
mod realm;
mod storage;
mod sync;
mod vault;
pub mod vfs_effects;
mod workload;

pub mod applet;
pub mod automation;
pub mod calls;
pub mod compute;
pub mod effects;
pub mod entropy;
pub mod errors;
pub mod events;
pub mod handle;
pub mod id;
pub mod id_mint;
pub mod keyspaces;
pub mod metadata;
pub mod metrics;
pub mod node;
pub mod operation;
#[cfg(any(test, feature = "test-hooks"))]
pub mod operation_check;
pub mod placement;
pub mod reducer;
pub mod register;
pub mod repository;
pub mod scheduling;
pub mod structs;
pub mod structured_id;
pub mod task;
pub mod telemetry;
#[cfg(any(test, feature = "test-hooks"))]
pub mod testing;
#[cfg(any(test, feature = "test-hooks"))]
pub mod time;
pub mod types;
pub mod user;
pub mod workflow;

pub use access::{
    admin_checks, admin_documents, audit, auth, config_authority, join_request, permission_path,
    request_policy,
};
pub use catalog::{
    archive_path, crate_merge, entity_patch, git, git_state, profile_shapes, repo_layout,
    rocrate_archive, search, search_enrichment,
};
pub use network::{
    alpn, backoff, dht_protocol, egress, listener, member_read, native_reference, network_config,
    outbound, replication_protocol, stream, trace_context,
};
pub use realm::{federation, handoff, link, onboarding, transfer};
pub use storage::{
    blob_paths, blob_records, bucket_read, cursor, key_index, live_version, persistence, row_plan,
    rows, staging_checkpoint, storage_entries, storage_read,
};
pub use sync::{
    deferred_topics, device_wipe, document, document_apply, heartbeat, notification_routing,
    sync_checks,
};
pub use vault::{
    credential_encryption, injection, key_seal, keys, password, realm_vault, recovery_code,
    secret_registers, secrets, vault_format,
};
pub use workload::{
    download_slots, install_work, job_request, jobs, managed_copy, plugin_installs, plugin_runs,
    plugins,
};

#[cfg(test)]
mod tests;
