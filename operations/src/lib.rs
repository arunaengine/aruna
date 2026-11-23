//! Domain state machines; the drivers crate runs them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Large operation types and trait bounds exceed the default recursion limit on fresh builds.
#![recursion_limit = "512"]
#![allow(clippy::large_enum_variant, clippy::result_large_err)]

pub use aruna_ops_data::{
    blob, connectors, device, distribution, git, holder_choice, metadata, replication, s3, shares,
    staging, transfer,
};
#[cfg(any(test, feature = "test-hooks"))]
pub use aruna_ops_data::{testing, testing_objects};
pub use aruna_ops_identity::{
    abe, auth, device_node, document_repository, forward, groups, node_vault, onboarding,
    placement, queue_backoff, realm, realm_vault, secrets, session, shard, sync, users,
};

pub mod assistant;
pub mod federation;
pub mod harvest;
pub mod jobs;
pub mod node;
pub mod notifications;
pub mod plugins;
pub mod relay;
pub mod triggers;

#[cfg(any(test, feature = "test-hooks"))]
pub mod tests;
