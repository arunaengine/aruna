//! Queues and retries repair work that mirrors or deletes synced bucket relationships.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    MIRROR_REPAIR_KEYSPACE, RELATIONSHIP_OUT_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::metadata::MetadataError;
use aruna_core::plugin_runs::{SourceEvent, journal_entries};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::{
    BucketInfo, bucket_permission_path, object_permission_path,
};
use aruna_core::structs::{SyncRelationship, SyncState, sync_relationship_key};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Key, KeySpace, TxnId, Value};
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::auth::request_authorization::{AuthorizeError, authorize};
use crate::auth::request_policy::PolicyRequestExtras;
use crate::driver::{DriverContext, drive};
use crate::metadata::AuthToken;
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::sync::sync_relationship::{
    DeleteRelationshipOperation, StoreRelationshipOperation, SyncRelationshipDirection,
    SyncRelationshipError, remove_outgoing_relationship,
};
use crate::tasks::queue_backoff::retry_delay_ms;

const REPAIR_PAGE_SIZE: usize = 128;
const REPAIR_BATCH_SIZE: usize = 64;
pub(crate) const RECONCILE_GRACE: Duration = Duration::from_secs(30);
pub const REPAIR_RETRY_AFTER: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SyncMirrorIntent {
    Reconcile,
    Delete,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncMirrorRecord {
    pub relationship: SyncRelationship,
    pub intent: SyncMirrorIntent,
    pub due_at_ms: u64,
    pub attempts: u32,
    pub last_error: Option<String>,
    /// A claimed rollback: the create never activated, so only the target mirror goes.
    pub rollback: bool,
}

impl SyncMirrorRecord {
    fn new(relationship: SyncRelationship, intent: SyncMirrorIntent) -> Self {
        let due_at_ms = match intent {
            SyncMirrorIntent::Reconcile => {
                unix_timestamp_millis().saturating_add(RECONCILE_GRACE.as_millis() as u64)
            }
            SyncMirrorIntent::Delete => unix_timestamp_millis(),
        };
        Self {
            relationship,
            intent,
            due_at_ms,
            attempts: 0,
            last_error: None,
            rollback: false,
        }
    }

    fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        self.relationship.validate()?;
        Ok(postcard::to_allocvec(self)?)
    }

    fn from_bytes(key: &[u8], value: &[u8]) -> Result<Self, ConversionError> {
        let record: Self = postcard::from_bytes(value)?;
        record.relationship.validate()?;
        if key != record.relationship.id.to_bytes() {
            return Err(ConversionError::FromStrError(
                "sync mirror repair key does not match payload".to_string(),
            ));
        }
        Ok(record)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SyncMirrorResult {
    pub processed: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub has_more_due: bool,
    pub next_due_after: Option<Duration>,
}

#[derive(Debug, Error)]
pub enum SyncMirrorError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Relationship(#[from] SyncRelationshipError),
    #[error("{0}")]
    Mirror(String),
    #[error("unexpected storage event: {0:?}")]
    Unexpected(Event),
}

/// What a reconcile pass finds when it tries to claim a rollback.
#[derive(Debug, PartialEq)]
enum RollbackClaim {
    /// The scanned record changed, so this pass has nothing to do.
    Stale,
    /// The relationship was activated and is reconciled as usual.
    Active(SyncRelationship),
    /// The create never activated; the record now asks for a rollback.
    Claimed,
}

struct RepairScan {
    records: Vec<(Vec<u8>, SyncMirrorRecord)>,
    has_more_due: bool,
    next_due_ms: Option<u64>,
}

pub fn mirror_delete_entry(
    relationship: &SyncRelationship,
) -> Result<(KeySpace, Key, Value), ConversionError> {
    repair_entry(SyncMirrorRecord::new(
        relationship.clone(),
        SyncMirrorIntent::Delete,
    ))
}

pub async fn stage_mirror_reconcile(
    context: &DriverContext,
    relationship: &SyncRelationship,
) -> Result<(), SyncMirrorError> {
    stage_mirror_intent(context, relationship, SyncMirrorIntent::Reconcile).await
}

pub async fn stage_mirror_delete(
    context: &DriverContext,
    relationship: &SyncRelationship,
) -> Result<(), SyncMirrorError> {
    stage_mirror_intent(context, relationship, SyncMirrorIntent::Delete).await
}

pub async fn store_sync_status(
    context: &DriverContext,
    relationship: &SyncRelationship,
) -> Result<bool, SyncMirrorError> {
    relationship.validate()?;
    let source_bucket = relationship
        .source
        .bucket()
        .ok_or_else(|| SyncMirrorError::Mirror("invalid source ARN".to_string()))?;
    let relationship_key = sync_relationship_key(source_bucket, relationship.id);
    let repair_key = relationship.id.to_bytes().to_vec();
    let txn_id = start_repair_transaction(&context.storage_handle).await?;
    let result = async {
        let existing =
            read_out_relationship(&context.storage_handle, &relationship_key, Some(txn_id)).await?;
        let Some(existing) = existing else {
            commit_repair_transaction(&context.storage_handle, txn_id).await?;
            return Ok(false);
        };
        if existing.state == SyncState::Detached {
            // A detached serving stub cannot be overwritten or restage its deleted mirror.
            commit_repair_transaction(&context.storage_handle, txn_id).await?;
            return Ok(false);
        }
        if !same_relationship(&existing, relationship) {
            return Err(SyncMirrorError::Mirror(
                "sync relationship identity changed during status update".to_string(),
            ));
        }
        if read_repair_record(&context.storage_handle, &repair_key, Some(txn_id))
            .await?
            .is_some_and(|record| record.intent == SyncMirrorIntent::Delete)
        {
            commit_repair_transaction(&context.storage_handle, txn_id).await?;
            return Ok(false);
        }

        let repair = SyncMirrorRecord::new(relationship.clone(), SyncMirrorIntent::Reconcile);
        let mut writes = vec![
            (
                RELATIONSHIP_OUT_KEYSPACE.to_string(),
                ByteView::from(relationship_key),
                ByteView::from(relationship.to_bytes()?),
            ),
            (
                MIRROR_REPAIR_KEYSPACE.to_string(),
                ByteView::from(repair_key),
                ByteView::from(repair.to_bytes()?),
            ),
        ];
        let failed = |state: &SyncState| matches!(state, SyncState::Failed { .. });
        if failed(&relationship.state) && !failed(&existing.state) {
            let stopped = failure_event(&context.storage_handle, relationship, txn_id).await?;
            writes.extend(
                stopped
                    .map(|event| journal_entries(&event))
                    .transpose()?
                    .into_iter()
                    .flatten(),
            );
        }
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                commit_repair_transaction(&context.storage_handle, txn_id).await?;
                Ok(true)
            }
            Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
            other => Err(SyncMirrorError::Unexpected(other)),
        }
    }
    .await;
    if result.is_err() {
        abort_repair_transaction(&context.storage_handle, txn_id).await;
    }
    result
}

pub async fn clear_mirror_repair(
    context: &DriverContext,
    relationship: &SyncRelationship,
    expected: SyncMirrorIntent,
) -> Result<(), SyncMirrorError> {
    clear_repair_intent(
        &context.storage_handle,
        relationship.id.to_bytes().to_vec(),
        relationship,
        expected,
    )
    .await
}

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

pub async fn kick_mirror_repair(context: &DriverContext) {
    let Some(task_handle) = context.task_handle.as_ref() else {
        return;
    };
    let event = task_handle
        .send_effect(Effect::Task(TaskEffect::ShortenTimer {
            key: TaskKey::DrainMirrorRepair,
            after: Duration::ZERO,
        }))
        .await;
    if let Event::Task(TaskEvent::Error { message, .. }) = event {
        warn!(%message, "Failed to schedule sync mirror repair");
    }
}

pub async fn ensure_sync_mirror(
    context: &DriverContext,
    local_node: NodeId,
    relationship: &SyncRelationship,
) -> Result<(), SyncMirrorError> {
    relationship.validate()?;
    if relationship.source.node_id != local_node {
        return Err(SyncMirrorError::Mirror(
            "sync mirror creation must run on the source node".to_string(),
        ));
    }
    let source_bucket = relationship
        .source
        .bucket()
        .ok_or_else(|| SyncMirrorError::Mirror("invalid source ARN".to_string()))?;
    let source_group_id =
        match drive(GetBucketOperation::new(source_bucket.to_string()), context).await {
            Ok(bucket_info) => bucket_info.group_id,
            Err(GetBucketError::NotFound) => {
                return Err(SyncMirrorError::Mirror(
                    "source bucket not found".to_string(),
                ));
            }
            Err(error) => return Err(SyncMirrorError::Mirror(error.to_string())),
        };
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
    if relationship.target.node_id == local_node {
        ensure_target_write(context, relationship).await?;
        return drive(
            StoreRelationshipOperation::new(
                relationship.clone(),
                SyncRelationshipDirection::Incoming,
            ),
            context,
        )
        .await
        .map(|_| ())
        .map_err(Into::into);
    }

    let metadata_handle = context
        .metadata_handle
        .as_ref()
        .ok_or_else(|| SyncMirrorError::Mirror("target unreachable".to_string()))?;
    metadata_handle
        .request_sync_create(
            relationship.target.node_id,
            Some(AuthToken::internal(AuthContext {
                user_id: relationship.created_by,
                realm_id: relationship.source.realm_id,
                path_restrictions: None,
                session: None,
            })),
            source_group_id,
            relationship.clone(),
            PolicyRequestExtras::operation("s3.PutBucketReplication"),
        )
        .await
        .map_err(|error| SyncMirrorError::Mirror(error.to_string()))
}

pub async fn delete_sync_mirror(
    context: &DriverContext,
    local_node: NodeId,
    relationship: &SyncRelationship,
) -> Result<(), SyncMirrorError> {
    send_mirror_delete(context, local_node, relationship, false).await
}

/// Removes the target mirror of a create that never activated, without the creator's WRITE.
async fn rollback_sync_mirror(
    context: &DriverContext,
    local_node: NodeId,
    relationship: &SyncRelationship,
) -> Result<(), SyncMirrorError> {
    send_mirror_delete(context, local_node, relationship, true).await
}

/// Claims the staged reconcile record of a new relationship inside `txn_id`. Rewriting the
/// record makes a concurrent rollback claim on the same key fail at commit.
pub(crate) async fn claim_activation(
    storage: &StorageHandle,
    relationship: &SyncRelationship,
    txn_id: TxnId,
) -> Result<(), SyncRelationshipError> {
    let key = relationship.id.to_bytes().to_vec();
    let value = match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: MIRROR_REPAIR_KEYSPACE.to_string(),
            key: ByteView::from(key.clone()),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => value,
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            return Err(SyncRelationshipError::RolledBack);
        }
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        received => {
            return Err(SyncRelationshipError::UnexpectedEvent {
                state: "ClaimingActivation",
                expected: "storage read result",
                received,
            });
        }
    };
    let record = SyncMirrorRecord::from_bytes(&key, &value)?;
    if record.intent != SyncMirrorIntent::Reconcile
        || !same_relationship(&record.relationship, relationship)
    {
        return Err(SyncRelationshipError::RolledBack);
    }
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: MIRROR_REPAIR_KEYSPACE.to_string(),
            key: ByteView::from(key),
            value,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        received => Err(SyncRelationshipError::UnexpectedEvent {
            state: "ClaimingActivation",
            expected: "storage write result",
            received,
        }),
    }
}

async fn send_mirror_delete(
    context: &DriverContext,
    local_node: NodeId,
    relationship: &SyncRelationship,
    rollback: bool,
) -> Result<(), SyncMirrorError> {
    relationship.validate()?;
    if relationship.source.node_id == local_node && relationship.target.node_id == local_node {
        if rollback {
            drive(
                DeleteRelationshipOperation::new(
                    relationship.clone(),
                    SyncRelationshipDirection::Incoming,
                ),
                context,
            )
            .await?;
        } else {
            delete_local_relationships(context, local_node, relationship).await?;
        }
        return Ok(());
    }

    let remote_node = if relationship.source.node_id == local_node {
        relationship.target.node_id
    } else if relationship.target.node_id == local_node && !rollback {
        relationship.source.node_id
    } else {
        return Err(SyncMirrorError::Mirror(
            "sync mirror deletion must run on an endpoint node".to_string(),
        ));
    };
    let metadata_handle = context
        .metadata_handle
        .as_ref()
        .ok_or_else(|| SyncMirrorError::Mirror("target unreachable".to_string()))?;
    match metadata_handle
        .request_sync_delete(
            remote_node,
            Some(AuthToken::internal(AuthContext {
                user_id: relationship.created_by,
                realm_id: relationship.source.realm_id,
                path_restrictions: None,
                session: None,
            })),
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

pub async fn process_mirror_repairs(
    context: &DriverContext,
    local_node: NodeId,
) -> Result<SyncMirrorResult, SyncMirrorError> {
    let now_ms = unix_timestamp_millis();
    let scan = scan_repair_records(&context.storage_handle, now_ms).await?;
    let mut succeeded = 0usize;
    let mut failed = 0usize;
    let mut claimed = false;

    for (key, record) in &scan.records {
        match process_repair_record(context, local_node, record).await {
            Ok(rollback) => {
                claimed |= rollback;
                clear_scanned_record(&context.storage_handle, key.clone(), record).await?;
                succeeded = succeeded.saturating_add(1);
            }
            Err(error) => {
                reschedule_repair_record(
                    &context.storage_handle,
                    key.clone(),
                    record,
                    error.to_string(),
                )
                .await?;
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

pub async fn restore_mirror_timer(storage: &StorageHandle, task_handle: &TaskHandle) {
    match next_repair_after(storage).await {
        Ok(None) => {}
        Ok(Some(after)) => {
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
        Err(error) => warn!(%error, "Failed to scan sync mirror repairs"),
    }
}

fn repair_entry(record: SyncMirrorRecord) -> Result<(KeySpace, Key, Value), ConversionError> {
    let key = record.relationship.id.to_bytes().to_vec();
    let value = record.to_bytes()?;
    Ok((
        MIRROR_REPAIR_KEYSPACE.to_string(),
        ByteView::from(key),
        ByteView::from(value),
    ))
}

async fn stage_mirror_intent(
    context: &DriverContext,
    relationship: &SyncRelationship,
    intent: SyncMirrorIntent,
) -> Result<(), SyncMirrorError> {
    store_repair_record(
        &context.storage_handle,
        SyncMirrorRecord::new(relationship.clone(), intent),
    )
    .await
}

async fn ensure_target_write(
    context: &DriverContext,
    relationship: &SyncRelationship,
) -> Result<(), SyncMirrorError> {
    let target_bucket = relationship
        .target
        .bucket()
        .ok_or_else(|| SyncMirrorError::Mirror("invalid target ARN".to_string()))?;
    let bucket_info = match drive(GetBucketOperation::new(target_bucket.to_string()), context).await
    {
        Ok(bucket_info) => bucket_info,
        Err(GetBucketError::NotFound) => {
            return Err(SyncMirrorError::Mirror(
                "target bucket not found".to_string(),
            ));
        }
        Err(error) => return Err(SyncMirrorError::Mirror(error.to_string())),
    };
    authorize_repair(
        context,
        relationship,
        relationship.target.realm_id,
        &bucket_permission_path(
            relationship.target.realm_id,
            bucket_info.group_id,
            relationship.target.node_id,
            target_bucket,
        ),
        &Permission::WRITE,
        "s3.PutBucketReplication",
    )
    .await
}

async fn authorize_repair(
    context: &DriverContext,
    relationship: &SyncRelationship,
    realm_id: aruna_core::structs::identity::realm::RealmId,
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
    match authorize(
        context,
        realm_id,
        &auth,
        path,
        permission,
        PolicyRequestExtras::operation(operation),
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

/// Returns true when the pass claimed a rollback that is due now.
async fn process_repair_record(
    context: &DriverContext,
    local_node: NodeId,
    record: &SyncMirrorRecord,
) -> Result<bool, SyncMirrorError> {
    match record.intent {
        SyncMirrorIntent::Reconcile => match settle_reconcile(&context.storage_handle, record)
            .await?
        {
            RollbackClaim::Stale => Ok(false),
            RollbackClaim::Claimed => Ok(true),
            // Detached stubs only serve retained reference reads; never
            // resurrect the target mirror for them.
            RollbackClaim::Active(relationship) if relationship.state == SyncState::Detached => {
                delete_sync_mirror(context, local_node, &record.relationship).await?;
                Ok(false)
            }
            RollbackClaim::Active(relationship) => {
                ensure_sync_mirror(context, local_node, &relationship).await?;
                Ok(false)
            }
        },
        // The claim found no outgoing record, so no local cleanup may leave a stub.
        SyncMirrorIntent::Delete if record.rollback => {
            rollback_sync_mirror(context, local_node, &record.relationship).await?;
            Ok(false)
        }
        SyncMirrorIntent::Delete => {
            delete_local_relationships(context, local_node, &record.relationship).await?;
            delete_sync_mirror(context, local_node, &record.relationship).await?;
            Ok(false)
        }
    }
}

async fn settle_reconcile(
    storage: &StorageHandle,
    record: &SyncMirrorRecord,
) -> Result<RollbackClaim, SyncMirrorError> {
    let txn_id = start_repair_transaction(storage).await?;
    let result = async {
        let claim = claim_rollback(storage, record, txn_id).await?;
        commit_repair_transaction(storage, txn_id).await?;
        Ok(claim)
    }
    .await;
    if result.is_err() {
        abort_repair_transaction(storage, txn_id).await;
    }
    result
}

/// Inside `txn_id`, turns a scanned reconcile record whose outgoing record is absent into a
/// rollback. Activation rewrites the same record, so only one of the two commits.
async fn claim_rollback(
    storage: &StorageHandle,
    record: &SyncMirrorRecord,
    txn_id: TxnId,
) -> Result<RollbackClaim, SyncMirrorError> {
    let key = record.relationship.id.to_bytes().to_vec();
    if read_repair_record(storage, &key, Some(txn_id))
        .await?
        .as_ref()
        != Some(record)
    {
        return Ok(RollbackClaim::Stale);
    }
    let source_bucket = record
        .relationship
        .source
        .bucket()
        .ok_or_else(|| SyncMirrorError::Mirror("invalid source ARN".to_string()))?;
    let relationship_key = sync_relationship_key(source_bucket, record.relationship.id);
    if let Some(existing) = read_out_relationship(storage, &relationship_key, Some(txn_id)).await? {
        return Ok(RollbackClaim::Active(existing));
    }
    let claimed = SyncMirrorRecord {
        rollback: true,
        ..SyncMirrorRecord::new(record.relationship.clone(), SyncMirrorIntent::Delete)
    };
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: MIRROR_REPAIR_KEYSPACE.to_string(),
            key: ByteView::from(key),
            value: ByteView::from(claimed.to_bytes()?),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(RollbackClaim::Claimed),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(SyncMirrorError::Unexpected(other)),
    }
}

async fn delete_local_relationships(
    context: &DriverContext,
    local_node: NodeId,
    relationship: &SyncRelationship,
) -> Result<(), SyncMirrorError> {
    if relationship.source.node_id == local_node {
        remove_outgoing_relationship(context, relationship.clone()).await?;
    }
    if relationship.target.node_id == local_node {
        drive(
            DeleteRelationshipOperation::new(
                relationship.clone(),
                SyncRelationshipDirection::Incoming,
            ),
            context,
        )
        .await?;
    }
    Ok(())
}

async fn scan_repair_records(
    storage: &StorageHandle,
    now_ms: u64,
) -> Result<RepairScan, SyncMirrorError> {
    let mut records = Vec::new();
    let mut next_due_ms: Option<u64> = None;
    let mut start_after = None;
    loop {
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: MIRROR_REPAIR_KEYSPACE.to_string(),
                prefix: None,
                start: start_after.take().map(IterStart::After),
                limit: REPAIR_PAGE_SIZE,
                txn_id: None,
            })
            .await;
        let (values, next_start_after) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            other => return Err(SyncMirrorError::Unexpected(other)),
        };

        for (key, value) in values {
            let record = match SyncMirrorRecord::from_bytes(&key, &value) {
                Ok(record) => record,
                Err(error) => {
                    warn!(%error, "Dropping malformed sync mirror repair record");
                    delete_repair_record(storage, key.to_vec()).await?;
                    continue;
                }
            };
            if record.due_at_ms <= now_ms {
                records.push((key.to_vec(), record));
            } else {
                next_due_ms = Some(
                    next_due_ms.map_or(record.due_at_ms, |current| current.min(record.due_at_ms)),
                );
            }
        }

        match next_start_after {
            Some(next) => start_after = Some(next),
            None => break,
        }
    }

    records.sort_by_key(|(key, record)| (record.due_at_ms, key.clone()));
    let has_more_due = records.len() > REPAIR_BATCH_SIZE;
    records.truncate(REPAIR_BATCH_SIZE);
    Ok(RepairScan {
        records,
        has_more_due,
        next_due_ms,
    })
}

async fn next_repair_after(storage: &StorageHandle) -> Result<Option<Duration>, SyncMirrorError> {
    let now_ms = unix_timestamp_millis();
    let scan = scan_repair_records(storage, now_ms).await?;
    if !scan.records.is_empty() || scan.has_more_due {
        return Ok(Some(Duration::ZERO));
    }
    Ok(scan
        .next_due_ms
        .map(|due_at| Duration::from_millis(due_at.saturating_sub(now_ms))))
}

async fn reschedule_repair_record(
    storage: &StorageHandle,
    key: Vec<u8>,
    record: &SyncMirrorRecord,
    error: String,
) -> Result<(), SyncMirrorError> {
    let attempts = record.attempts.saturating_add(1);
    let next = SyncMirrorRecord {
        relationship: record.relationship.clone(),
        intent: record.intent,
        due_at_ms: unix_timestamp_millis().saturating_add(retry_delay_ms(attempts)),
        attempts,
        last_error: Some(error),
        rollback: record.rollback,
    };
    let txn_id = start_repair_transaction(storage).await?;
    let result = async {
        if read_repair_record(storage, &key, Some(txn_id))
            .await?
            .as_ref()
            != Some(record)
        {
            return commit_repair_transaction(storage, txn_id).await;
        }
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: MIRROR_REPAIR_KEYSPACE.to_string(),
                key: ByteView::from(key),
                value: ByteView::from(next.to_bytes()?),
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {
                commit_repair_transaction(storage, txn_id).await
            }
            Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
            other => Err(SyncMirrorError::Unexpected(other)),
        }
    }
    .await;
    if result.is_err() {
        abort_repair_transaction(storage, txn_id).await;
    }
    result
}

async fn store_repair_record(
    storage: &StorageHandle,
    mut record: SyncMirrorRecord,
) -> Result<(), SyncMirrorError> {
    let key = record.relationship.id.to_bytes().to_vec();
    let txn_id = start_repair_transaction(storage).await?;
    let result = async {
        let existing = read_repair_record(storage, &key, Some(txn_id)).await?;
        if record.intent == SyncMirrorIntent::Reconcile
            && existing
                .as_ref()
                .is_some_and(|existing| existing.intent == SyncMirrorIntent::Delete)
        {
            return commit_repair_transaction(storage, txn_id).await;
        }
        // A delete staged after a refused activation keeps the claimed rollback.
        if existing.is_some_and(|existing| {
            existing.rollback && same_relationship(&existing.relationship, &record.relationship)
        }) {
            record.rollback = true;
        }
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: MIRROR_REPAIR_KEYSPACE.to_string(),
                key: ByteView::from(key),
                value: ByteView::from(record.to_bytes()?),
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {
                commit_repair_transaction(storage, txn_id).await
            }
            Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
            other => Err(SyncMirrorError::Unexpected(other)),
        }
    }
    .await;
    if result.is_err() {
        abort_repair_transaction(storage, txn_id).await;
    }
    result
}

async fn clear_repair_intent(
    storage: &StorageHandle,
    key: Vec<u8>,
    relationship: &SyncRelationship,
    expected: SyncMirrorIntent,
) -> Result<(), SyncMirrorError> {
    let txn_id = start_repair_transaction(storage).await?;
    let result = async {
        let existing = read_repair_record(storage, &key, Some(txn_id)).await?;
        if existing.is_none_or(|existing| {
            existing.intent != expected
                || (expected == SyncMirrorIntent::Reconcile
                    && existing.relationship != *relationship)
        }) {
            return commit_repair_transaction(storage, txn_id).await;
        }
        match storage
            .send_storage_effect(StorageEffect::Delete {
                key_space: MIRROR_REPAIR_KEYSPACE.to_string(),
                key: ByteView::from(key),
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::DeleteResult { .. }) => {
                commit_repair_transaction(storage, txn_id).await
            }
            Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
            other => Err(SyncMirrorError::Unexpected(other)),
        }
    }
    .await;
    if result.is_err() {
        abort_repair_transaction(storage, txn_id).await;
    }
    result
}

async fn clear_scanned_record(
    storage: &StorageHandle,
    key: Vec<u8>,
    expected: &SyncMirrorRecord,
) -> Result<(), SyncMirrorError> {
    let txn_id = start_repair_transaction(storage).await?;
    let result = async {
        if read_repair_record(storage, &key, Some(txn_id))
            .await?
            .as_ref()
            != Some(expected)
        {
            return commit_repair_transaction(storage, txn_id).await;
        }
        match storage
            .send_storage_effect(StorageEffect::Delete {
                key_space: MIRROR_REPAIR_KEYSPACE.to_string(),
                key: ByteView::from(key),
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::DeleteResult { .. }) => {
                commit_repair_transaction(storage, txn_id).await
            }
            Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
            other => Err(SyncMirrorError::Unexpected(other)),
        }
    }
    .await;
    if result.is_err() {
        abort_repair_transaction(storage, txn_id).await;
    }
    result
}

async fn read_repair_record(
    storage: &StorageHandle,
    key: &[u8],
    txn_id: Option<TxnId>,
) -> Result<Option<SyncMirrorRecord>, SyncMirrorError> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: MIRROR_REPAIR_KEYSPACE.to_string(),
            key: ByteView::from(key.to_vec()),
            txn_id,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => SyncMirrorRecord::from_bytes(key, &value)
            .map(Some)
            .map_err(Into::into),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(SyncMirrorError::Unexpected(other)),
    }
}

async fn read_out_relationship(
    storage: &StorageHandle,
    key: &[u8],
    txn_id: Option<TxnId>,
) -> Result<Option<SyncRelationship>, SyncMirrorError> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: RELATIONSHIP_OUT_KEYSPACE.to_string(),
            key: ByteView::from(key.to_vec()),
            txn_id,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => SyncRelationship::from_bytes(&value)
            .map(Some)
            .map_err(Into::into),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(SyncMirrorError::Unexpected(other)),
    }
}

/// The plugin event of a relationship that stops as failed, from the source bucket row its status
/// transaction reads; none once that bucket is gone.
async fn failure_event(
    storage: &StorageHandle,
    relationship: &SyncRelationship,
    txn_id: TxnId,
) -> Result<Option<SourceEvent>, SyncMirrorError> {
    let source = &relationship.source;
    let bucket = source.bucket().unwrap_or_default();
    let read = StorageEffect::Read {
        key_space: S3_BUCKET_KEYSPACE.to_string(),
        key: ByteView::from(bucket.as_bytes().to_vec()),
        txn_id: Some(txn_id),
    };
    let info = match storage.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => return Ok(None),
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => BucketInfo::from_bytes(&value)?,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        other => return Err(SyncMirrorError::Unexpected(other)),
    };
    let prefix = source.key_prefix().unwrap_or_default();
    let path = object_permission_path(
        source.realm_id,
        info.group_id,
        source.node_id,
        bucket,
        prefix,
    );
    Ok(Some(SourceEvent::sync_failure(
        (info.group_id, path),
        (relationship.id, relationship.created_by),
        (source.node_id, Ulid::generate()),
        unix_timestamp_millis(),
    )))
}

fn same_relationship(left: &SyncRelationship, right: &SyncRelationship) -> bool {
    left.id == right.id
        && left.source == right.source
        && left.target == right.target
        && left.mode == right.mode
        && left.reference_handling == right.reference_handling
        && left.reference_serving == right.reference_serving
        && left.replicate_deletes == right.replicate_deletes
        && left.created_by == right.created_by
        && left.created_at == right.created_at
}

async fn start_repair_transaction(storage: &StorageHandle) -> Result<TxnId, SyncMirrorError> {
    match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => Ok(txn_id),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(SyncMirrorError::Unexpected(other)),
    }
}

async fn commit_repair_transaction(
    storage: &StorageHandle,
    txn_id: TxnId,
) -> Result<(), SyncMirrorError> {
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(SyncMirrorError::Unexpected(other)),
    }
}

async fn abort_repair_transaction(storage: &StorageHandle, txn_id: TxnId) {
    let _ = storage
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
}

async fn delete_repair_record(
    storage: &StorageHandle,
    key: Vec<u8>,
) -> Result<(), SyncMirrorError> {
    match storage
        .send_storage_effect(StorageEffect::Delete {
            key_space: MIRROR_REPAIR_KEYSPACE.to_string(),
            key: ByteView::from(key),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(SyncMirrorError::Unexpected(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::sync_relationship::{
        GetRelationshipOperation, activate_relationship, create_sync_relationship,
    };
    use aruna_core::UserId;
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE,
    };
    use aruna_core::request_policy::{PolicyKind, RequestPolicy};
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
    use aruna_core::structs::identity::realm::{
        RealmAuthorizationDocument, RealmConfigDocument, RealmId,
    };
    use aruna_core::structs::storage::blob::BucketInfo;
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::structs::storage::replication::ArunaArn;
    use aruna_core::structs::{ReferenceHandling, SyncMode, SyncState, SyncStatusSnapshot};
    use aruna_storage::FjallStorage;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn relationship() -> SyncRelationship {
        let realm_id = RealmId::from_bytes([1; 32]);
        SyncRelationship {
            id: Ulid::from_bytes([2; 16]),
            source: ArunaArn::s3_bucket(realm_id, node(3), "source").unwrap(),
            target: ArunaArn::s3_bucket(realm_id, node(4), "target").unwrap(),
            mode: SyncMode::Continuous,
            reference_handling: Default::default(),
            reference_serving: false,
            replicate_deletes: true,
            created_by: UserId::local(Ulid::from_bytes([5; 16]), realm_id),
            created_at: SystemTime::UNIX_EPOCH,
            state: SyncState::Enabled,
            status: SyncStatusSnapshot::default(),
        }
    }

    fn policy(expression: &str) -> RequestPolicy {
        RequestPolicy {
            policy_id: Ulid::from_bytes([7; 16]),
            name: "mirror-test".to_string(),
            kind: PolicyKind::Deny,
            when: None,
            expression: expression.to_string(),
            enabled: true,
        }
    }

    async fn setup_auth(expression: Option<&str>) -> (tempfile::TempDir, DriverContext) {
        let tempdir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let relationship = relationship();
        let realm_id = relationship.source.realm_id;
        let group_id = Ulid::from_bytes([6; 16]);
        let actor = Actor {
            node_id: relationship.source.node_id,
            user_id: relationship.created_by,
            realm_id,
        };
        let auth_doc = GroupAuthorizationDocument::default_group_doc(
            relationship.created_by,
            realm_id,
            group_id,
        );
        let group = Group {
            display_name: "mirror-test".to_string(),
            group_id,
            realm_id,
            roles: auth_doc.roles.keys().copied().collect(),
            owner: relationship.created_by,
        };
        let mut realm = RealmConfigDocument::new(realm_id, Vec::new(), 1);
        realm.request_policies = expression.map(policy).into_iter().collect();
        let source_bucket = relationship.source.bucket().unwrap();
        let bucket = relationship.target.bucket().unwrap();
        let bucket_info = BucketInfo {
            group_id,
            created_at: relationship.created_at,
            created_by: relationship.created_by,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        let event = storage
            .send_storage_effect(StorageEffect::BatchWrite {
                writes: vec![
                    (
                        REALM_CONFIG_KEYSPACE.to_string(),
                        realm_id.as_bytes().to_vec().into(),
                        realm.to_bytes(&actor).unwrap().into(),
                    ),
                    (
                        GROUP_KEYSPACE.to_string(),
                        group_id.to_bytes().to_vec().into(),
                        group.to_bytes(&actor).unwrap().into(),
                    ),
                    (
                        AUTH_KEYSPACE.to_string(),
                        realm_id.as_bytes().to_vec().into(),
                        RealmAuthorizationDocument::default_realm_doc(realm_id)
                            .to_bytes(&actor)
                            .unwrap()
                            .into(),
                    ),
                    (
                        AUTH_KEYSPACE.to_string(),
                        group_id.to_bytes().to_vec().into(),
                        auth_doc.to_bytes(&actor).unwrap().into(),
                    ),
                    (
                        S3_BUCKET_KEYSPACE.to_string(),
                        source_bucket.as_bytes().into(),
                        bucket_info.to_bytes().unwrap().into(),
                    ),
                    (
                        S3_BUCKET_KEYSPACE.to_string(),
                        bucket.as_bytes().into(),
                        bucket_info.to_bytes().unwrap().into(),
                    ),
                ],
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::BatchWriteResult { .. })
        ));
        (tempdir, context)
    }

    fn storage_context() -> (tempfile::TempDir, DriverContext) {
        let tempdir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        (tempdir, context)
    }

    async fn staged_record(
        context: &DriverContext,
        relationship: &SyncRelationship,
    ) -> SyncMirrorRecord {
        stage_mirror_reconcile(context, relationship).await.unwrap();
        read_repair_record(&context.storage_handle, &relationship.id.to_bytes(), None)
            .await
            .unwrap()
            .unwrap()
    }

    async fn stored_record(
        context: &DriverContext,
        relationship: &SyncRelationship,
    ) -> SyncMirrorRecord {
        read_repair_record(&context.storage_handle, &relationship.id.to_bytes(), None)
            .await
            .unwrap()
            .unwrap()
    }

    async fn outgoing(
        context: &DriverContext,
        relationship: &SyncRelationship,
    ) -> Option<SyncRelationship> {
        let key = sync_relationship_key(relationship.source.bucket().unwrap(), relationship.id);
        read_out_relationship(&context.storage_handle, &key, None)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn rollback_blocks_activation() {
        let (_tempdir, context) = storage_context();
        let relationship = relationship();
        let record = staged_record(&context, &relationship).await;

        assert_eq!(
            settle_reconcile(&context.storage_handle, &record)
                .await
                .unwrap(),
            RollbackClaim::Claimed
        );
        assert_eq!(
            create_sync_relationship(&context, relationship.clone()).await,
            Err(SyncRelationshipError::RolledBack)
        );
        assert_eq!(
            activate_relationship(&context, relationship.clone()).await,
            Err(SyncRelationshipError::RolledBack)
        );

        assert_eq!(outgoing(&context, &relationship).await, None);
        let claimed = stored_record(&context, &relationship).await;
        assert_eq!(claimed.intent, SyncMirrorIntent::Delete);
        assert!(claimed.rollback);
    }

    #[tokio::test]
    async fn activation_blocks_rollback() {
        let (_tempdir, context) = storage_context();
        let relationship = relationship();
        let record = staged_record(&context, &relationship).await;

        create_sync_relationship(&context, relationship.clone())
            .await
            .unwrap();

        assert_eq!(
            settle_reconcile(&context.storage_handle, &record)
                .await
                .unwrap(),
            RollbackClaim::Active(relationship.clone())
        );
        assert_eq!(stored_record(&context, &relationship).await, record);
        assert_eq!(outgoing(&context, &relationship).await, Some(relationship));
    }

    #[tokio::test]
    async fn claims_conflict() {
        // Both orders: the claim that commits second must fail and change nothing.
        for activation_first in [true, false] {
            let (_tempdir, context) = storage_context();
            let storage = &context.storage_handle;
            let relationship = relationship();
            let record = staged_record(&context, &relationship).await;
            let activation = start_repair_transaction(storage).await.unwrap();
            let rollback = start_repair_transaction(storage).await.unwrap();

            claim_activation(storage, &relationship, activation)
                .await
                .unwrap();
            let key = sync_relationship_key(relationship.source.bucket().unwrap(), relationship.id);
            assert!(matches!(
                storage
                    .send_storage_effect(StorageEffect::Write {
                        key_space: RELATIONSHIP_OUT_KEYSPACE.to_string(),
                        key: key.into(),
                        value: relationship.to_bytes().unwrap().into(),
                        txn_id: Some(activation),
                    })
                    .await,
                Event::Storage(StorageEvent::WriteResult { .. })
            ));
            assert_eq!(
                claim_rollback(storage, &record, rollback).await.unwrap(),
                RollbackClaim::Claimed
            );
            let (first, second) = match activation_first {
                true => (activation, rollback),
                false => (rollback, activation),
            };
            commit_repair_transaction(storage, first).await.unwrap();

            assert!(matches!(
                commit_repair_transaction(storage, second).await,
                Err(SyncMirrorError::Storage(StorageError::TransactionConflict))
            ));
            let stored = stored_record(&context, &relationship).await;
            if activation_first {
                assert_eq!(stored, record);
                assert_eq!(outgoing(&context, &relationship).await, Some(relationship));
            } else {
                assert!(stored.rollback);
                assert_eq!(outgoing(&context, &relationship).await, None);
            }
        }
    }

    #[tokio::test]
    async fn delete_keeps_rollback() {
        let (_tempdir, context) = storage_context();
        let relationship = relationship();
        let record = staged_record(&context, &relationship).await;
        settle_reconcile(&context.storage_handle, &record)
            .await
            .unwrap();

        stage_mirror_delete(&context, &relationship).await.unwrap();

        let stored = stored_record(&context, &relationship).await;
        assert_eq!(stored.intent, SyncMirrorIntent::Delete);
        assert!(stored.rollback);
    }

    #[tokio::test]
    async fn rollback_skips_stub() {
        // A reference-serving create that never activated must not leave a detached stub.
        for mode in [SyncMode::Reference, SyncMode::Continuous] {
            let (_tempdir, context) = storage_context();
            let mut relationship = relationship();
            relationship.mode = mode;
            relationship.set_reference_handling(ReferenceHandling::Preserve);
            let record = staged_record(&context, &relationship).await;
            settle_reconcile(&context.storage_handle, &record)
                .await
                .unwrap();

            let result = process_mirror_repairs(&context, relationship.source.node_id)
                .await
                .unwrap();

            assert_eq!(result.failed, 1);
            assert_eq!(outgoing(&context, &relationship).await, None);
            assert_eq!(
                stored_record(&context, &relationship).await.last_error,
                Some("target unreachable".to_string())
            );
        }
    }

    #[test]
    fn repair_record_roundtrips() {
        let record = SyncMirrorRecord::new(relationship(), SyncMirrorIntent::Reconcile);
        let key = record.relationship.id.to_bytes();

        assert_eq!(
            SyncMirrorRecord::from_bytes(&key, &record.to_bytes().unwrap()).unwrap(),
            record
        );
    }

    #[test]
    fn serving_changes_identity() {
        let left = relationship();
        let mut right = left.clone();
        right.reference_serving = true;

        assert!(!same_relationship(&left, &right));
    }

    #[tokio::test]
    async fn delete_supersedes_reconcile() {
        let tempdir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let relationship = relationship();

        stage_mirror_reconcile(&context, &relationship)
            .await
            .unwrap();
        stage_mirror_delete(&context, &relationship).await.unwrap();
        stage_mirror_reconcile(&context, &relationship)
            .await
            .unwrap();

        let Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) = storage
            .send_storage_effect(StorageEffect::Read {
                key_space: MIRROR_REPAIR_KEYSPACE.to_string(),
                key: relationship.id.to_bytes().to_vec().into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing mirror repair record");
        };
        let record = SyncMirrorRecord::from_bytes(&relationship.id.to_bytes(), &value).unwrap();
        assert_eq!(record.intent, SyncMirrorIntent::Delete);
    }

    #[tokio::test]
    async fn delete_blocks_status() {
        let tempdir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let relationship = relationship();
        drive(
            StoreRelationshipOperation::new(
                relationship.clone(),
                SyncRelationshipDirection::Outgoing,
            ),
            &context,
        )
        .await
        .unwrap();
        stage_mirror_delete(&context, &relationship).await.unwrap();
        let mut updated = relationship.clone();
        updated.status.last_error = Some("failed".to_string());

        assert!(!store_sync_status(&context, &updated).await.unwrap());
        assert_eq!(
            drive(
                GetRelationshipOperation::new(
                    relationship.id,
                    SyncRelationshipDirection::Outgoing,
                ),
                &context,
            )
            .await
            .unwrap(),
            relationship
        );
    }

    #[tokio::test]
    async fn status_stages_reconcile() {
        let tempdir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let mut relationship = relationship();
        relationship.state = SyncState::Failed {
            reason: "failed".to_string(),
        };
        drive(
            StoreRelationshipOperation::new(
                relationship.clone(),
                SyncRelationshipDirection::Outgoing,
            ),
            &context,
        )
        .await
        .unwrap();
        relationship.state = SyncState::Enabled;
        relationship.status.last_error = None;

        assert!(store_sync_status(&context, &relationship).await.unwrap());
        assert_eq!(
            drive(
                GetRelationshipOperation::new(
                    relationship.id,
                    SyncRelationshipDirection::Outgoing,
                ),
                &context,
            )
            .await
            .unwrap(),
            relationship
        );
        assert_eq!(
            read_repair_record(&storage, &relationship.id.to_bytes(), None)
                .await
                .unwrap()
                .map(|record| record.relationship),
            Some(relationship)
        );
    }

    /// The plugin journal rows this node has not matched yet.
    async fn journaled(context: &DriverContext) -> Vec<SourceEvent> {
        let txn = crate::plugins::store::Txn::direct(context);
        let pending = aruna_core::plugin_runs::PENDING_ROW;
        let page = crate::plugins::journal::journal_page(&txn, pending, None, 100).await;
        page.unwrap()
            .0
            .into_iter()
            .map(|(_, event)| event)
            .collect()
    }

    async fn stored(context: &DriverContext, relationship: &SyncRelationship) {
        let direction = SyncRelationshipDirection::Outgoing;
        let store = StoreRelationshipOperation::new(relationship.clone(), direction);
        drive(store, context).await.unwrap();
    }

    #[tokio::test]
    async fn failure_journals_once() {
        // A relationship that stops as failed journals one `sync_failed` row with its status;
        // storing it failed again writes none, and a failure after a resume writes a second.
        let (_dir, context) = setup_auth(None).await;
        let mut relationship = relationship();
        stored(&context, &relationship).await;
        let failed = SyncState::Failed {
            reason: "access_denied".to_string(),
        };
        relationship.state = failed.clone();
        assert!(store_sync_status(&context, &relationship).await.unwrap());
        relationship.status.counters.failures = 2;
        assert!(store_sync_status(&context, &relationship).await.unwrap());

        let events = journaled(&context).await;
        assert_eq!(events.len(), 1);
        let source = &relationship.source;
        let group_id = Ulid::from_bytes([6; 16]);
        let path = object_permission_path(source.realm_id, group_id, source.node_id, "source", "");
        assert_eq!(
            (events[0].kind, &events[0].path),
            (aruna_core::plugin_runs::SourceKind::SyncFailed, &path)
        );
        assert_eq!(events[0].resource, relationship.id.to_string());
        let prefix = format!("sync_failed/{}/{}/", source.node_id, relationship.id);
        assert!(events[0].source.starts_with(&prefix));

        relationship.state = SyncState::Enabled;
        assert!(store_sync_status(&context, &relationship).await.unwrap());
        relationship.state = failed;
        assert!(store_sync_status(&context, &relationship).await.unwrap());
        let events = journaled(&context).await;
        assert_eq!(events.len(), 2);
        assert_ne!(events[0].source, events[1].source);
    }

    #[tokio::test]
    async fn failure_needs_commit() {
        // An aborted status transaction, a detached stub and a deleted source bucket journal
        // nothing.
        let failed = SyncState::Failed {
            reason: "access_denied".to_string(),
        };
        let (_dir, context) = setup_auth(None).await;
        let relationship = relationship();
        stored(&context, &relationship).await;
        let mut changed = relationship.clone();
        changed.replicate_deletes = false;
        changed.state = failed.clone();
        assert!(store_sync_status(&context, &changed).await.is_err());
        let mut detached = relationship.clone();
        detached.state = SyncState::Detached;
        stored(&context, &detached).await;
        detached.state = failed.clone();
        assert!(!store_sync_status(&context, &detached).await.unwrap());
        assert!(journaled(&context).await.is_empty());

        let tempdir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let mut relationship = relationship;
        stored(&context, &relationship).await;
        relationship.state = failed;
        assert!(store_sync_status(&context, &relationship).await.unwrap());
        assert!(journaled(&context).await.is_empty());
    }

    #[tokio::test]
    async fn status_stays_handling() {
        let tempdir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let mut current = relationship();
        let stale = current.clone();
        current.set_reference_handling(ReferenceHandling::Preserve);
        drive(
            StoreRelationshipOperation::new(current.clone(), SyncRelationshipDirection::Outgoing),
            &context,
        )
        .await
        .unwrap();

        assert!(matches!(
            store_sync_status(&context, &stale).await,
            Err(SyncMirrorError::Mirror(_))
        ));
        assert_eq!(
            drive(
                GetRelationshipOperation::new(current.id, SyncRelationshipDirection::Outgoing,),
                &context,
            )
            .await
            .unwrap(),
            current
        );
    }

    #[tokio::test]
    async fn stale_reconcile_survives() {
        let tempdir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let relationship = relationship();
        let mut stale = SyncMirrorRecord::new(relationship.clone(), SyncMirrorIntent::Reconcile);
        stale.due_at_ms = 0;
        store_repair_record(&storage, stale.clone()).await.unwrap();

        let mut current = stale.clone();
        current.relationship.status.last_error = Some("newer".to_string());
        current.due_at_ms = 10;
        store_repair_record(&storage, current.clone())
            .await
            .unwrap();
        let key = relationship.id.to_bytes().to_vec();

        clear_scanned_record(&storage, key.clone(), &stale)
            .await
            .unwrap();
        reschedule_repair_record(&storage, key.clone(), &stale, "stale".to_string())
            .await
            .unwrap();

        assert_eq!(
            read_repair_record(&storage, &key, None).await.unwrap(),
            Some(current)
        );
    }

    #[tokio::test]
    async fn newer_reconcile_survives() {
        let tempdir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let stale = relationship();
        let mut current = stale.clone();
        current.status.last_error = Some("newer".to_string());
        store_repair_record(
            &storage,
            SyncMirrorRecord::new(current.clone(), SyncMirrorIntent::Reconcile),
        )
        .await
        .unwrap();

        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        clear_mirror_repair(&context, &stale, SyncMirrorIntent::Reconcile)
            .await
            .unwrap();

        let key = current.id.to_bytes().to_vec();
        assert_eq!(
            read_repair_record(&storage, &key, None)
                .await
                .unwrap()
                .map(|record| record.relationship),
            Some(current)
        );
    }

    #[tokio::test]
    async fn delete_clear_preserved() {
        let tempdir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let stale = relationship();
        let mut current = stale.clone();
        current.status.last_error = Some("newer".to_string());
        store_repair_record(
            &storage,
            SyncMirrorRecord::new(current, SyncMirrorIntent::Delete),
        )
        .await
        .unwrap();

        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        clear_mirror_repair(&context, &stale, SyncMirrorIntent::Delete)
            .await
            .unwrap();

        assert!(
            read_repair_record(&storage, &stale.id.to_bytes(), None)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn repair_backs_off() {
        // Authorized repair reaches the missing metadata handle and retries.
        let (_tempdir, context) = setup_auth(None).await;
        let storage = context.storage_handle.clone();
        let relationship = relationship();
        drive(
            StoreRelationshipOperation::new(
                relationship.clone(),
                SyncRelationshipDirection::Outgoing,
            ),
            &context,
        )
        .await
        .unwrap();
        stage_mirror_reconcile(&context, &relationship)
            .await
            .unwrap();

        let mut record = SyncMirrorRecord::new(relationship.clone(), SyncMirrorIntent::Reconcile);
        record.due_at_ms = 0;
        let (key_space, key, value) = repair_entry(record).unwrap();
        assert!(matches!(
            storage
                .send_storage_effect(StorageEffect::Write {
                    key_space,
                    key,
                    value,
                    txn_id: None,
                })
                .await,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));

        let result = process_mirror_repairs(&context, relationship.source.node_id)
            .await
            .unwrap();
        assert_eq!(result.failed, 1);

        let Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) = storage
            .send_storage_effect(StorageEffect::Read {
                key_space: MIRROR_REPAIR_KEYSPACE.to_string(),
                key: relationship.id.to_bytes().to_vec().into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing mirror repair retry");
        };
        let record = SyncMirrorRecord::from_bytes(&relationship.id.to_bytes(), &value).unwrap();
        assert_eq!(record.attempts, 1);
        assert_eq!(record.last_error.as_deref(), Some("target unreachable"));
    }

    #[tokio::test]
    async fn policy_denies_write() {
        let (_tempdir, context) = setup_auth(Some("operation == 's3.PutBucketReplication'")).await;

        let result = ensure_target_write(&context, &relationship()).await;

        assert!(matches!(
            result,
            Err(SyncMirrorError::Mirror(error)) if error == "access_denied"
        ));
    }

    #[tokio::test]
    async fn policy_allows_write() {
        let (_tempdir, context) = setup_auth(Some("operation == 's3.GetObject'")).await;

        assert!(ensure_target_write(&context, &relationship()).await.is_ok());
    }

    #[tokio::test]
    async fn policy_retry_survives() {
        let (_tempdir, context) = setup_auth(Some("operation == 's3.PutBucketReplication'")).await;
        let mut relationship = relationship();
        relationship.target = ArunaArn::s3_bucket(
            relationship.target.realm_id,
            relationship.source.node_id,
            "target",
        )
        .unwrap();
        drive(
            StoreRelationshipOperation::new(
                relationship.clone(),
                SyncRelationshipDirection::Outgoing,
            ),
            &context,
        )
        .await
        .unwrap();
        let mut record = SyncMirrorRecord::new(relationship.clone(), SyncMirrorIntent::Reconcile);
        record.due_at_ms = 0;
        store_repair_record(&context.storage_handle, record)
            .await
            .unwrap();

        let result = process_mirror_repairs(&context, relationship.source.node_id)
            .await
            .unwrap();

        assert_eq!(result.failed, 1);
        assert_eq!(
            read_repair_record(&context.storage_handle, &relationship.id.to_bytes(), None,)
                .await
                .unwrap()
                .and_then(|record| record.last_error),
            Some("access_denied".to_string())
        );
    }
}
