//! Reads metadata documents and references, checking that each record is live and readable.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::query_fanout::DistributedQueryOperation;
use super::read_types::MetadataQueryRequest;
use super::{
    AuthContext, AuthorizationError, CheckPermissionsConfig, CheckPermissionsOperation, Event,
    GroupId, MetadataApiError, MetadataFanoutScope, MetadataRegistryRecord, NodeId, Permission,
    RealmId, StorageEffect, StorageEvent, StorageReadError, TxnId, Ulid, query_fingerprint,
};
use crate::driver::{DriverContext, drive};
use crate::metadata::record_reads::DocumentRecordOperation;

use crate::auth::enforcement::EnforcePolicyOperation;
use crate::metadata::live_records::{LiveRecordsError, LiveRecordsOperation};

/// The query a caller asked for, across the realm or on this node as its mode says.
pub fn query_metadata(
    realm_id: RealmId,
    local_node_id: NodeId,
    request: MetadataQueryRequest,
) -> DistributedQueryOperation {
    let subject = query_fingerprint(
        &request.query,
        request.graph_iris.as_deref(),
        request.mode,
        None,
        None,
    );
    let scope = MetadataFanoutScope::new(request.mode, request.target_nodes, request.allow_partial)
        .with_subject(subject);
    DistributedQueryOperation::request(
        (realm_id, local_node_id),
        (request.auth, request.bearer_token),
        (request.graph_iris, request.query),
        scope,
    )
}

impl From<LiveRecordsError> for MetadataApiError {
    fn from(error: LiveRecordsError) -> Self {
        match error {
            LiveRecordsError::TooMany => Self::ServiceUnavailable,
            other => Self::Internal(other.to_string()),
        }
    }
}

pub(crate) async fn load_live_record(
    context: &DriverContext,
    document_id: Ulid,
) -> Result<MetadataRegistryRecord, MetadataApiError> {
    match drive(DocumentRecordOperation::new(document_id), context).await {
        Ok(Some(record)) => drive(LiveRecordsOperation::new(vec![record], None), context)
            .await?
            .into_iter()
            .next()
            .ok_or(MetadataApiError::NotFound),
        Ok(None) => Err(MetadataApiError::NotFound),
        Err(StorageReadError::Storage(error)) => Err(MetadataApiError::Internal(error.to_string())),
        Err(StorageReadError::Conversion(error)) => {
            Err(MetadataApiError::Internal(error.to_string()))
        }
    }
}

pub(crate) async fn ensure_record_readable(
    context: &DriverContext,
    realm_id: RealmId,
    auth: Option<&AuthContext>,
    record: &MetadataRegistryRecord,
    txn_id: Option<TxnId>,
) -> Result<(), MetadataApiError> {
    if record.public {
        // A policy denial on a found public record must read as NotFound, matching
        // the private-denied path, so read-by-id is not an existence oracle.
        let request = crate::auth::request_policy::policy_request_with(
            &record.permission_path,
            &Permission::READ,
            auth,
            crate::auth::request_policy::PolicyRequestExtras::operation("metadata.read"),
        );
        let result = match txn_id {
            Some(txn_id) => policies_in(context, realm_id, record.group_id, txn_id)
                .await
                .and_then(|evaluator| evaluator.evaluate(&request)),
            None => {
                drive(
                    EnforcePolicyOperation::new(realm_id, request.clone()),
                    context,
                )
                .await
            }
        };
        return result.map_err(|_| MetadataApiError::NotFound);
    }
    // Unreadable and absent records both return NotFound to prevent existence probing.
    let Some(auth) = auth.cloned() else {
        return Err(MetadataApiError::NotFound);
    };
    match ensure_permission(
        context,
        realm_id,
        auth,
        record.group_id,
        record.permission_path.clone(),
        Permission::READ,
        txn_id,
    )
    .await
    {
        Ok(()) => Ok(()),
        Err(MetadataApiError::Forbidden | MetadataApiError::Unauthorized) => {
            Err(MetadataApiError::NotFound)
        }
        Err(other) => Err(other),
    }
}

pub(super) async fn ensure_permission(
    context: &DriverContext,
    realm_id: RealmId,
    auth: AuthContext,
    group_id: GroupId,
    path: String,
    required_permission: Permission,
    txn_id: Option<TxnId>,
) -> Result<(), MetadataApiError> {
    if auth.realm_id != realm_id {
        return Err(MetadataApiError::Forbidden);
    }
    let config = CheckPermissionsConfig {
        auth_context: auth.clone(),
        path: path.clone(),
        required_permission: required_permission.clone(),
    };
    let checked = async {
        match txn_id {
            Some(txn_id) => {
                let values = rows_in(context, config.reads()?, txn_id).await?;
                config.allowed(values)
            }
            None => drive(CheckPermissionsOperation::new(config), context).await,
        }
    };
    let allowed = aruna_core::telemetry::time_stage("permission", checked)
        .await
        .map_err(|err| match err {
            AuthorizationError::InvalidRealmId
            | AuthorizationError::InvalidGroupId
            | AuthorizationError::GroupNotFound
            | AuthorizationError::DocNotFound => MetadataApiError::Forbidden,
            _ => MetadataApiError::Internal(err.to_string()),
        })?;
    if !allowed {
        return Err(MetadataApiError::Forbidden);
    }
    // Policies must see the permission the RBAC check enforced; a fixed read
    // would let a write-deny policy pass unevaluated.
    let request = crate::auth::request_policy::policy_request_with(
        &path,
        &required_permission,
        Some(&auth),
        crate::auth::request_policy::PolicyRequestExtras::operation("metadata.read"),
    );
    match txn_id {
        Some(txn_id) => policies_in(context, realm_id, group_id, txn_id)
            .await
            .and_then(|evaluator| evaluator.evaluate(&request))
            .map_err(|_| MetadataApiError::Forbidden)?,
        None => drive(
            EnforcePolicyOperation::new(realm_id, request.clone()),
            context,
        )
        .await
        .map_err(|_| MetadataApiError::Forbidden)?,
    }
    Ok(())
}

/// Reads `reads` inside the caller's transaction and answers their values in that order.
async fn rows_in(
    context: &DriverContext,
    reads: Vec<(String, aruna_core::types::Key)>,
    txn_id: TxnId,
) -> Result<Vec<Option<aruna_core::types::Value>>, aruna_core::errors::StorageError> {
    let read = StorageEffect::BatchRead {
        reads,
        txn_id: Some(txn_id),
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::BatchReadResult { values }) => {
            Ok(values.into_iter().map(|(_, value)| value).collect())
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error),
        other => Err(aruna_core::errors::StorageError::ReadError(
            other.label().to_string(),
        )),
    }
}

/// The realm's and the group's policies, read inside the caller's transaction.
async fn policies_in(
    context: &DriverContext,
    realm_id: RealmId,
    group_id: GroupId,
    txn_id: TxnId,
) -> Result<crate::auth::enforcement::PolicyEvaluator, crate::auth::error::PolicyEnforcementError> {
    use crate::auth::enforcement::{policy_reads, read_policies};
    use crate::auth::error::PolicyEnforcementError;
    let reads = policy_reads(realm_id, Some(group_id));
    let values = rows_in(context, reads, txn_id)
        .await
        .map_err(|error| PolicyEnforcementError::Unavailable(error.to_string()))?;
    read_policies(realm_id, Some(group_id), values)
}
