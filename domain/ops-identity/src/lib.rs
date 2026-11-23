//! Identity, realm, auth, group, placement, shard and sync state machines.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Large operation types and trait bounds exceed the default recursion limit on fresh builds.
#![recursion_limit = "512"]
#![allow(clippy::large_enum_variant, clippy::result_large_err)]

pub mod abe;
pub mod auth;
pub mod device_node;
pub mod document_repository;
pub mod endpoint_screening;
pub mod forward;
pub mod groups;
pub mod node_vault;
pub mod onboarding;
pub mod owner_index;
pub mod placement;
pub mod queue_backoff;
pub mod realm;
pub mod realm_vault;
pub mod secrets;
pub mod session;
pub mod shard;
#[cfg(any(test, feature = "test-hooks"))]
pub mod stepper;
pub mod sync;
#[cfg(any(test, feature = "test-hooks"))]
pub mod testing;
#[cfg(any(test, feature = "test-hooks"))]
pub mod tests;
pub mod users;

/// Deterministic member order for records that several nodes must byte-match.
pub(crate) fn sorted_user_ids(
    user_ids: &std::collections::HashSet<aruna_core::user::id::UserId>,
) -> Vec<aruna_core::user::id::UserId> {
    let mut user_ids: Vec<_> = user_ids.iter().copied().collect();
    user_ids.sort();
    user_ids
}
