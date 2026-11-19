//! Groups the bucket policy operations: bulk apply, coverage scan, mutation and successor mint.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod bulk;
pub mod count;
pub mod coverage;
pub mod mutation;
pub mod successor;

/// A child check that the caller may write `path`.
pub(crate) fn write_check(
    auth_context: aruna_core::structs::identity::auth::AuthContext,
    path: String,
) -> aruna_core::calls::IoCall {
    use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
    aruna_core::calls::IoCall::child(CheckPermissionsOperation::new(CheckPermissionsConfig {
        auth_context,
        path,
        required_permission: aruna_core::structs::identity::auth::Permission::WRITE,
    }))
}
