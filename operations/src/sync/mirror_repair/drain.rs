//! Runs due mirror repairs; creating and deleting the remote mirror still needs async peer calls.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_core::metadata::MetadataError;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::bucket_permission_path;
use aruna_core::structs::{SyncRelationship, SyncState};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::time::unix_timestamp_millis;
use aruna_tasks::TaskHandle;
use tracing::warn;
use ulid::Ulid;

use super::record::MirrorRecordOperation;
use super::scan::ScanRepairsOperation;
use super::settle::{RollbackClaim, SettleReconcileOperation};
use super::{
    MirrorPeer, SyncMirrorError, SyncMirrorIntent, SyncMirrorRecord, SyncMirrorResult, create_peer,
    delete_peer,
};
use crate::auth::error::AuthorizeError;
use crate::auth::request_authorization::AuthorizeOperation;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::driver::{DriverContext, drive};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::sync::sync_relationship::{
    DeleteRelationshipOperation, RemoveOutgoingOperation, StoreRelationshipOperation,
    SyncRelationshipDirection,
};
use aruna_core::metadata::AuthToken;

/// Forwards a user-authorized sync mirror creation request to the target
/// node so routes never orchestrate metadata effects directly.
pub async fn request_mirror_create(
    context: &DriverContext,
    target_node: NodeId,
    auth_token: AuthToken,
    source_group_id: Ulid,
    relationship: SyncRelationship,
    extras: PolicyRequestExtras,
) -> Result<(), MetadataError> {
    let metadata_handle = context
        .metadata_handle
        .as_ref()
        .ok_or(MetadataError::HandleMissing)?;
    metadata_handle
        .request_sync_create(
            target_node,
            Some(auth_token),
            source_group_id,
            relationship,
            extras,
        )
        .await
}

pub async fn delete_sync_mirror(
    context: &DriverContext,
    local_node: NodeId,
    relationship: &SyncRelationship,
) -> Result<(), SyncMirrorError> {
    send_mirror_delete(context, local_node, relationship, false).await
}

pub async fn process_mirror_repairs(
    context: &DriverContext,
    local_node: NodeId,
) -> Result<SyncMirrorResult, SyncMirrorError> {
    let now_ms = unix_timestamp_millis();
    let scan = drive(ScanRepairsOperation::new(), context).await?;
    let mut succeeded = 0usize;
    let mut failed = 0usize;
    let mut claimed = false;

    for record in &scan.records {
        match process_repair_record(context, local_node, record).await {
            Ok(rollback) => {
                claimed |= rollback;
                drive(
                    MirrorRecordOperation::clear_scanned(record.clone()),
                    context,
                )
                .await?;
                succeeded = succeeded.saturating_add(1);
            }
            Err(error) => {
                let reschedule =
                    MirrorRecordOperation::reschedule(record.clone(), error.to_string());
                drive(reschedule, context).await?;
                failed = failed.saturating_add(1);
            }
        }
    }

    // A claimed rollback is due now, so the next pass runs at once.
    let has_more_due = scan.has_more_due || claimed;
    Ok(SyncMirrorResult {
        processed: scan.records.len(),
        succeeded,
        failed,
        has_more_due,
        next_due_after: if has_more_due {
            None
        } else {
            scan.next_due_ms
                .map(|due_at| Duration::from_millis(due_at.saturating_sub(now_ms)))
        },
    })
}

/// Re-arms the repair timer at startup from the stored records.
pub async fn restore_mirror_timer(context: &DriverContext, task_handle: &TaskHandle) {
    let now_ms = unix_timestamp_millis();
    let after = match drive(ScanRepairsOperation::new(), context).await {
        Ok(scan) if !scan.records.is_empty() || scan.has_more_due => Duration::ZERO,
        Ok(scan) => match scan.next_due_ms {
            Some(due_at) => Duration::from_millis(due_at.saturating_sub(now_ms)),
            None => return,
        },
        Err(error) => {
            warn!(%error, "Failed to scan sync mirror repairs");
            return;
        }
    };
    let event = task_handle
        .send_effect(Effect::Task(TaskEffect::ResetTimer {
            key: TaskKey::DrainMirrorRepair,
            after,
        }))
        .await;
    if let Event::Task(TaskEvent::Error { message, .. }) = event {
        warn!(%message, "Failed to restore sync mirror repair timer");
    }
}

/// Returns true when the pass claimed a rollback that is due now.
async fn process_repair_record(
    context: &DriverContext,
    local_node: NodeId,
    record: &SyncMirrorRecord,
) -> Result<bool, SyncMirrorError> {
    match record.intent {
        SyncMirrorIntent::Reconcile => {
            match drive(SettleReconcileOperation::new(record.clone()), context).await? {
                RollbackClaim::Stale => Ok(false),
                RollbackClaim::Claimed => Ok(true),
                // Detached stubs only serve retained reference reads; never
                // resurrect the target mirror for them.
                RollbackClaim::Active(relationship)
                    if relationship.state == SyncState::Detached =>
                {
                    delete_sync_mirror(context, local_node, &record.relationship).await?;
                    Ok(false)
                }
                RollbackClaim::Active(relationship) => {
                    ensure_sync_mirror(context, local_node, &relationship).await?;
                    Ok(false)
                }
            }
        }
        // The claim found no outgoing record, so no local cleanup may leave a stub.
        SyncMirrorIntent::Delete if record.rollback => {
            send_mirror_delete(context, local_node, &record.relationship, true).await?;
            Ok(false)
        }
        SyncMirrorIntent::Delete => {
            delete_local_relationships(context, local_node, &record.relationship).await?;
            delete_sync_mirror(context, local_node, &record.relationship).await?;
            Ok(false)
        }
    }
}

async fn ensure_sync_mirror(
    context: &DriverContext,
    local_node: NodeId,
    relationship: &SyncRelationship,
) -> Result<(), SyncMirrorError> {
    relationship.validate()?;
    let peer = create_peer(relationship, local_node)?;
    let source_bucket = relationship
        .source
        .bucket()
        .ok_or_else(|| SyncMirrorError::Mirror("invalid source ARN".to_string()))?;
    let source_group_id = bucket_group(context, source_bucket, "source").await?;
    authorize_repair(
        context,
        relationship,
        relationship.source.realm_id,
        &bucket_permission_path(
            relationship.source.realm_id,
            source_group_id,
            relationship.source.node_id,
            source_bucket,
        ),
        &Permission::READ,
        "s3.GetObject",
    )
    .await?;
    let MirrorPeer::Remote(target) = peer else {
        ensure_target_write(context, relationship).await?;
        let store = StoreRelationshipOperation::new(
            relationship.clone(),
            SyncRelationshipDirection::Incoming,
        );
        return drive(store, context).await.map(|_| ()).map_err(Into::into);
    };

    let metadata_handle = context
        .metadata_handle
        .as_ref()
        .ok_or_else(|| SyncMirrorError::Mirror("target unreachable".to_string()))?;
    metadata_handle
        .request_sync_create(
            target,
            Some(creator_token(relationship)),
            source_group_id,
            relationship.clone(),
            PolicyRequestExtras::operation("s3.PutBucketReplication"),
        )
        .await
        .map_err(|error| SyncMirrorError::Mirror(error.to_string()))
}

/// Deletes the remote or local mirror. A rollback removes only the target mirror of a create
/// that never activated, without the creator's WRITE.
async fn send_mirror_delete(
    context: &DriverContext,
    local_node: NodeId,
    relationship: &SyncRelationship,
    rollback: bool,
) -> Result<(), SyncMirrorError> {
    relationship.validate()?;
    let MirrorPeer::Remote(remote_node) = delete_peer(relationship, local_node, rollback)? else {
        if rollback {
            let delete = DeleteRelationshipOperation::new(
                relationship.clone(),
                SyncRelationshipDirection::Incoming,
            );
            drive(delete, context).await?;
        } else {
            delete_local_relationships(context, local_node, relationship).await?;
        }
        return Ok(());
    };
    let metadata_handle = context
        .metadata_handle
        .as_ref()
        .ok_or_else(|| SyncMirrorError::Mirror("target unreachable".to_string()))?;
    match metadata_handle
        .request_sync_delete(
            remote_node,
            Some(creator_token(relationship)),
            relationship.clone(),
            PolicyRequestExtras::operation("s3.DeleteBucketReplication"),
            rollback,
        )
        .await
    {
        Ok(()) => Ok(()),
        Err(MetadataError::Backend(message)) if message == "not_found" => Ok(()),
        Err(error) => Err(SyncMirrorError::Mirror(error.to_string())),
    }
}

pub(super) async fn ensure_target_write(
    context: &DriverContext,
    relationship: &SyncRelationship,
) -> Result<(), SyncMirrorError> {
    let target_bucket = relationship
        .target
        .bucket()
        .ok_or_else(|| SyncMirrorError::Mirror("invalid target ARN".to_string()))?;
    let group_id = bucket_group(context, target_bucket, "target").await?;
    authorize_repair(
        context,
        relationship,
        relationship.target.realm_id,
        &bucket_permission_path(
            relationship.target.realm_id,
            group_id,
            relationship.target.node_id,
            target_bucket,
        ),
        &Permission::WRITE,
        "s3.PutBucketReplication",
    )
    .await
}

/// The owning group of a local endpoint bucket; `side` names the endpoint in the error.
async fn bucket_group(
    context: &DriverContext,
    bucket: &str,
    side: &str,
) -> Result<Ulid, SyncMirrorError> {
    match drive(GetBucketOperation::new(bucket.to_string()), context).await {
        Ok(bucket_info) => Ok(bucket_info.group_id),
        Err(GetBucketError::NotFound) => {
            Err(SyncMirrorError::Mirror(format!("{side} bucket not found")))
        }
        Err(error) => Err(SyncMirrorError::Mirror(error.to_string())),
    }
}

fn creator_token(relationship: &SyncRelationship) -> AuthToken {
    AuthToken::internal(AuthContext {
        user_id: relationship.created_by,
        realm_id: relationship.source.realm_id,
        path_restrictions: None,
        session: None,
    })
}

async fn authorize_repair(
    context: &DriverContext,
    relationship: &SyncRelationship,
    realm_id: RealmId,
    path: &str,
    permission: &Permission,
    operation: &'static str,
) -> Result<(), SyncMirrorError> {
    let auth = AuthContext {
        user_id: relationship.created_by,
        realm_id: relationship.created_by.realm_id,
        path_restrictions: None,
        session: None,
    };
    match drive(
        AuthorizeOperation::new(
            realm_id,
            &auth,
            path,
            permission,
            PolicyRequestExtras::operation(operation),
        ),
        context,
    )
    .await
    {
        Ok(()) => Ok(()),
        Err(AuthorizeError::PermissionDenied | AuthorizeError::Policy(_)) => {
            Err(SyncMirrorError::Mirror("access_denied".to_string()))
        }
        Err(AuthorizeError::CheckFailed(error)) => Err(SyncMirrorError::Mirror(error)),
        Err(AuthorizeError::Storage(error)) => Err(SyncMirrorError::Mirror(error.to_string())),
    }
}

async fn delete_local_relationships(
    context: &DriverContext,
    local_node: NodeId,
    relationship: &SyncRelationship,
) -> Result<(), SyncMirrorError> {
    if relationship.source.node_id == local_node {
        drive(RemoveOutgoingOperation::new(relationship.clone()), context).await?;
    }
    if relationship.target.node_id == local_node {
        let delete = DeleteRelationshipOperation::new(
            relationship.clone(),
            SyncRelationshipDirection::Incoming,
        );
        drive(delete, context).await?;
    }
    Ok(())
}
