//! Domain state machines and their adapters over the operation runner.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Fresh (non-incremental) builds overflow the default query depth in the
// deeply nested async state machines; incremental builds mask it.
#![recursion_limit = "512"]
#![allow(clippy::large_enum_variant, clippy::result_large_err)]

pub mod assistant;
pub mod auth;
pub mod blob;
pub mod connectors;
pub mod device;
pub mod document_repository;
pub mod driver;
pub(crate) mod effect_adapters;
pub(crate) mod endpoint_screening;
pub mod forward;
pub mod git;
pub mod groups;
pub mod harvest;
pub mod jobs;
pub mod metadata;
pub mod node;
pub mod notifications;
pub mod onboarding;
mod owner_index;
pub mod placement;
pub mod realm;
pub mod replication;
pub mod s3;
pub mod session;
pub mod shard;
pub mod staging;
mod storage_read;
pub mod sync;
pub mod tasks;
pub mod users;

#[cfg(test)]
mod tests;

/// Deterministic member order for records that several nodes must byte-match.
pub(crate) fn sorted_user_ids(
    user_ids: &std::collections::HashSet<aruna_core::UserId>,
) -> Vec<aruna_core::UserId> {
    let mut user_ids: Vec<_> = user_ids.iter().copied().collect();
    user_ids.sort();
    user_ids
}
