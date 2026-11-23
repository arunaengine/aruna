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

/// This node's prepare or cancel decision on `plan`, for the tests of other crates.
#[cfg(any(test, feature = "test-hooks"))]
pub fn prepare_operation(
    plan: aruna_core::structs::identity::group_delete::GroupDeletePlan,
    phase: aruna_core::structs::identity::group_delete::GroupDeletePhase,
    node_id: aruna_core::id::NodeId,
    now: std::time::SystemTime,
) -> impl aruna_core::operation::Operation<Output = (), Error = GroupDeletionError> + 'static {
    prepare::PrepareOperation::new(plan, phase, node_id, now)
}

/// The commit of `certificate` by `actor`, for the tests of other crates.
#[cfg(any(test, feature = "test-hooks"))]
pub fn commit_operation(
    certificate: aruna_core::structs::identity::group_delete::GroupDeleteCertificate,
    actor: aruna_core::structs::identity::auth::Actor,
    auth: aruna_core::structs::identity::auth::AuthContext,
) -> impl aruna_core::operation::Operation<
    Output = aruna_core::admin_documents::AdminDocumentEvent,
    Error = GroupDeletionError,
> + 'static {
    commit::CommitOperation::new(certificate, actor, Some(auth), None)
}
