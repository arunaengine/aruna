//! Coordinates empty group deletion and commits durable per-node decisions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod access;
pub mod check;
mod commit;
pub mod coordinator;
mod onboarding;
pub mod peer;
mod prepare;
mod resources;

use aruna_core::structs::identity::group_delete::GroupDeletionError;

#[cfg(test)]
mod barrier_tests;
#[cfg(test)]
mod check_tests;
#[cfg(test)]
mod recovery_tests;
#[cfg(test)]
mod tests;

/// The reads of the permission rules on `path`.
fn auth_call(path: &str) -> Result<aruna_core::calls::IoCall, GroupDeletionError> {
    crate::auth::permission_rules::rule_reads(path)
        .map(aruna_core::calls::IoCall::Read)
        .map_err(|error| GroupDeletionError::Unavailable(error.to_string()))
}
