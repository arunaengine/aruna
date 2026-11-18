//! Loads the permission rules and policies that decide which source objects a scope may read.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::errors::AuthorizationError;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::types::GroupId;

use super::version_replication::{SourceAuthorization, SourceAuthorizationError};
use crate::auth::enforcement::PolicyLoadOperation;
use crate::auth::permission_rules::{PermissionRulesConfig, PermissionRulesOperation};
use crate::driver::{DriverContext, drive};

impl SourceAuthorization {
    pub(crate) async fn load(
        context: &DriverContext,
        auth_context: AuthContext,
        group_id: GroupId,
        source_node_id: NodeId,
    ) -> Result<Self, SourceAuthorizationError> {
        let permissions = drive(
            PermissionRulesOperation::new(PermissionRulesConfig {
                auth_context: auth_context.clone(),
                path: format!("/{}/g/{group_id}", auth_context.realm_id),
            }),
            context,
        )
        .await
        .map_err(permission_error)?;
        let policies = drive(
            PolicyLoadOperation::new(auth_context.realm_id, Some(group_id)),
            context,
        )
        .await
        .map_err(|error| SourceAuthorizationError::Unavailable(error.to_string()))?;
        Ok(Self {
            group_id,
            source_node_id,
            auth_context,
            permissions,
            policies,
        })
    }
}

fn permission_error(error: AuthorizationError) -> SourceAuthorizationError {
    match error {
        AuthorizationError::DocNotFound
        | AuthorizationError::GroupNotFound
        | AuthorizationError::InvalidGroupId
        | AuthorizationError::InvalidRealmId => SourceAuthorizationError::Denied,
        other => SourceAuthorizationError::Unavailable(other.to_string()),
    }
}
