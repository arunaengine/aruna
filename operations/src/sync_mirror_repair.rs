use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{SYNC_MIRROR_REPAIR_KEYSPACE, SYNC_RELATIONSHIP_OUT_KEYSPACE};
use aruna_core::metadata::MetadataError;
use aruna_core::structs::{
    AuthContext, Permission, SyncRelationship, blob_bucket_permission_path, sync_relationship_key,
};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Key, KeySpace, TxnId, Value};
use aruna_core::util::unix_timestamp_millis;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::driver::{DriverContext, drive};
use crate::metadata::MetadataAuthToken;
use crate::queue_backoff::queue_retry_after_ms;
use crate::s3::get_bucket_info::GetBucketInfoOperation;
use crate::sync_relationship::{
    DeleteSyncRelationshipOperation, GetSyncRelationshipOperation, StoreSyncRelationshipOperation,
    SyncRelationshipDirection, SyncRelationshipError,
};

const REPAIR_PAGE_SIZE: usize = 128;
const REPAIR_BATCH_SIZE: usize = 64;
pub(crate) const RECONCILE_GRACE: Duration = Duration::from_secs(30);
pub const MIRROR_REPAIR_RETRY_AFTER: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SyncMirrorRepairIntent {
    Reconcile,
    Delete,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncMirrorRepairRecord {
    pub relationship: SyncRelationship,
    pub intent: SyncMirrorRepairIntent,
    pub due_at_ms: u64,
    pub attempts: u32,
    pub last_error: Option<String>,
}

impl SyncMirrorRepairRecord {
    fn new(relationship: SyncRelationship, intent: SyncMirrorRepairIntent) -> Self {
        let due_at_ms = match intent {
            SyncMirrorRepairIntent::Reconcile => {
                unix_timestamp_millis().saturating_add(RECONCILE_GRACE.as_millis() as u64)
            }
            SyncMirrorRepairIntent::Delete => unix_timestamp_millis(),
        };
        Self {
            relationship,
            intent,
            due_at_ms,
            attempts: 0,
            last_error: None,
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
pub struct SyncMirrorRepairResult {
    pub processed: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub has_more_due: bool,
    pub next_due_after: Option<Duration>,
}

#[derive(Debug, Error)]
pub enum SyncMirrorRepairError {
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

struct RepairScan {
    records: Vec<(Vec<u8>, SyncMirrorRepairRecord)>,
    has_more_due: bool,
    next_due_at_ms: Option<u64>,
}

pub fn mirror_delete_entry(
    relationship: &SyncRelationship,
) -> Result<(KeySpace, Key, Value), ConversionError> {
    repair_entry(SyncMirrorRepairRecord::new(
        relationship.clone(),
        SyncMirrorRepairIntent::Delete,
    ))
}

pub async fn stage_mirror_reconcile(
    context: &DriverContext,
    relationship: &SyncRelationship,
) -> Result<(), SyncMirrorRepairError> {
    stage_mirror_intent(context, relationship, SyncMirrorRepairIntent::Reconcile).await
}

pub async fn stage_mirror_delete(
    context: &DriverContext,
    relationship: &SyncRelationship,
) -> Result<(), SyncMirrorRepairError> {
    stage_mirror_intent(context, relationship, SyncMirrorRepairIntent::Delete).await
}

pub async fn store_sync_status(
    context: &DriverContext,
    relationship: &SyncRelationship,
) -> Result<bool, SyncMirrorRepairError> {
    relationship.validate()?;
    let source_bucket = relationship
        .source
        .bucket()
        .ok_or_else(|| SyncMirrorRepairError::Mirror("invalid source ARN".to_string()))?;
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
        if !same_relationship(&existing, relationship) {
            return Err(SyncMirrorRepairError::Mirror(
                "sync relationship identity changed during status update".to_string(),
            ));
        }
        if read_repair_record(&context.storage_handle, &repair_key, Some(txn_id))
            .await?
            .is_some_and(|record| record.intent == SyncMirrorRepairIntent::Delete)
        {
            commit_repair_transaction(&context.storage_handle, txn_id).await?;
            return Ok(false);
        }

        let repair =
            SyncMirrorRepairRecord::new(relationship.clone(), SyncMirrorRepairIntent::Reconcile);
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::BatchWrite {
                writes: vec![
                    (
                        SYNC_RELATIONSHIP_OUT_KEYSPACE.to_string(),
                        ByteView::from(relationship_key),
                        ByteView::from(relationship.to_bytes()?),
                    ),
                    (
                        SYNC_MIRROR_REPAIR_KEYSPACE.to_string(),
                        ByteView::from(repair_key),
                        ByteView::from(repair.to_bytes()?),
                    ),
                ],
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                commit_repair_transaction(&context.storage_handle, txn_id).await?;
                Ok(true)
            }
            Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
            other => Err(SyncMirrorRepairError::Unexpected(other)),
        }
    }
    .await;
    if result.is_err() {
        abort_repair_transaction(&context.storage_handle, txn_id).await;
    }
    result
}

pub async fn clear_mirror_repair(
    storage: &StorageHandle,
    relationship: &SyncRelationship,
    expected: SyncMirrorRepairIntent,
) -> Result<(), SyncMirrorRepairError> {
    clear_repair_intent(
        storage,
        relationship.id.to_bytes().to_vec(),
        relationship,
        expected,
    )
    .await
}

pub async fn kick_mirror_repair(context: &DriverContext) {
    let Some(task_handle) = context.task_handle.as_ref() else {
        return;
    };
    let event = task_handle
        .send_effect(Effect::Task(TaskEffect::ShortenTimer {
            key: TaskKey::DrainSyncMirrorRepair,
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
) -> Result<(), SyncMirrorRepairError> {
    relationship.validate()?;
    if relationship.source.node_id != local_node {
        return Err(SyncMirrorRepairError::Mirror(
            "sync mirror creation must run on the source node".to_string(),
        ));
    }
    if relationship.target.node_id == local_node {
        ensure_target_write(context, relationship).await?;
        return drive(
            StoreSyncRelationshipOperation::new(
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
        .ok_or_else(|| SyncMirrorRepairError::Mirror("target unreachable".to_string()))?;
    let source_bucket = relationship
        .source
        .bucket()
        .ok_or_else(|| SyncMirrorRepairError::Mirror("invalid source ARN".to_string()))?;
    let source_group_id = match drive(
        GetBucketInfoOperation::new(source_bucket.to_string()),
        context,
    )
    .await
    {
        Ok(Some(Ok(bucket_info))) => bucket_info.group_id,
        Ok(Some(Err(error))) => return Err(SyncMirrorRepairError::Mirror(error.to_string())),
        Ok(None) => {
            return Err(SyncMirrorRepairError::Mirror(
                "source bucket not found".to_string(),
            ));
        }
        Err(error) => return Err(SyncMirrorRepairError::Mirror(error.to_string())),
    };
    metadata_handle
        .request_sync_create(
            relationship.target.node_id,
            Some(MetadataAuthToken::internal(
                relationship.created_by,
                relationship.source.realm_id,
            )),
            source_group_id,
            relationship.clone(),
        )
        .await
        .map_err(|error| SyncMirrorRepairError::Mirror(error.to_string()))
}

pub async fn delete_sync_mirror(
    context: &DriverContext,
    local_node: NodeId,
    relationship: &SyncRelationship,
) -> Result<(), SyncMirrorRepairError> {
    relationship.validate()?;
    if relationship.source.node_id == local_node && relationship.target.node_id == local_node {
        delete_local_relationships(context, local_node, relationship).await?;
        return Ok(());
    }

    let remote_node = if relationship.source.node_id == local_node {
        relationship.target.node_id
    } else if relationship.target.node_id == local_node {
        relationship.source.node_id
    } else {
        return Err(SyncMirrorRepairError::Mirror(
            "sync mirror deletion must run on an endpoint node".to_string(),
        ));
    };
    let metadata_handle = context
        .metadata_handle
        .as_ref()
        .ok_or_else(|| SyncMirrorRepairError::Mirror("target unreachable".to_string()))?;
    match metadata_handle
        .request_sync_delete(
            remote_node,
            Some(MetadataAuthToken::internal(
                relationship.created_by,
                relationship.source.realm_id,
            )),
            relationship.clone(),
        )
        .await
    {
        Ok(()) => Ok(()),
        Err(MetadataError::Backend(message)) if message == "not_found" => Ok(()),
        Err(error) => Err(SyncMirrorRepairError::Mirror(error.to_string())),
    }
}

pub async fn process_mirror_repairs(
    context: &DriverContext,
    local_node: NodeId,
) -> Result<SyncMirrorRepairResult, SyncMirrorRepairError> {
    let now_ms = unix_timestamp_millis();
    let scan = scan_repair_records(&context.storage_handle, now_ms).await?;
    let mut succeeded = 0usize;
    let mut failed = 0usize;

    for (key, record) in &scan.records {
        match process_repair_record(context, local_node, record).await {
            Ok(()) => {
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

    Ok(SyncMirrorRepairResult {
        processed: scan.records.len(),
        succeeded,
        failed,
        has_more_due: scan.has_more_due,
        next_due_after: if scan.has_more_due {
            None
        } else {
            scan.next_due_at_ms
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
                    key: TaskKey::DrainSyncMirrorRepair,
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

fn repair_entry(record: SyncMirrorRepairRecord) -> Result<(KeySpace, Key, Value), ConversionError> {
    let key = record.relationship.id.to_bytes().to_vec();
    let value = record.to_bytes()?;
    Ok((
        SYNC_MIRROR_REPAIR_KEYSPACE.to_string(),
        ByteView::from(key),
        ByteView::from(value),
    ))
}

async fn stage_mirror_intent(
    context: &DriverContext,
    relationship: &SyncRelationship,
    intent: SyncMirrorRepairIntent,
) -> Result<(), SyncMirrorRepairError> {
    store_repair_record(
        &context.storage_handle,
        SyncMirrorRepairRecord::new(relationship.clone(), intent),
    )
    .await
}

async fn ensure_target_write(
    context: &DriverContext,
    relationship: &SyncRelationship,
) -> Result<(), SyncMirrorRepairError> {
    let target_bucket = relationship
        .target
        .bucket()
        .ok_or_else(|| SyncMirrorRepairError::Mirror("invalid target ARN".to_string()))?;
    let bucket_info = match drive(
        GetBucketInfoOperation::new(target_bucket.to_string()),
        context,
    )
    .await
    {
        Ok(Some(Ok(bucket_info))) => bucket_info,
        Ok(Some(Err(error))) => return Err(SyncMirrorRepairError::Mirror(error.to_string())),
        Ok(None) => {
            return Err(SyncMirrorRepairError::Mirror(
                "target bucket not found".to_string(),
            ));
        }
        Err(error) => return Err(SyncMirrorRepairError::Mirror(error.to_string())),
    };
    let permitted = drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: AuthContext {
                user_id: relationship.created_by,
                realm_id: relationship.created_by.realm_id,
                path_restrictions: None,
            },
            path: blob_bucket_permission_path(
                relationship.target.realm_id,
                bucket_info.group_id,
                relationship.target.node_id,
                target_bucket,
            ),
            required_permission: Permission::WRITE,
        }),
        context,
    )
    .await
    .map_err(|error| SyncMirrorRepairError::Mirror(error.to_string()))?;
    if permitted {
        Ok(())
    } else {
        Err(SyncMirrorRepairError::Mirror("access_denied".to_string()))
    }
}

async fn process_repair_record(
    context: &DriverContext,
    local_node: NodeId,
    record: &SyncMirrorRepairRecord,
) -> Result<(), SyncMirrorRepairError> {
    match record.intent {
        SyncMirrorRepairIntent::Reconcile => match drive(
            GetSyncRelationshipOperation::new(
                record.relationship.id,
                SyncRelationshipDirection::Outgoing,
            ),
            context,
        )
        .await
        {
            Ok(relationship) => ensure_sync_mirror(context, local_node, &relationship).await,
            Err(SyncRelationshipError::NotFound) => {
                delete_sync_mirror(context, local_node, &record.relationship).await
            }
            Err(error) => Err(error.into()),
        },
        SyncMirrorRepairIntent::Delete => {
            delete_local_relationships(context, local_node, &record.relationship).await?;
            delete_sync_mirror(context, local_node, &record.relationship).await
        }
    }
}

async fn delete_local_relationships(
    context: &DriverContext,
    local_node: NodeId,
    relationship: &SyncRelationship,
) -> Result<(), SyncMirrorRepairError> {
    if relationship.source.node_id == local_node {
        drive(
            DeleteSyncRelationshipOperation::new(
                relationship.clone(),
                SyncRelationshipDirection::Outgoing,
            ),
            context,
        )
        .await?;
    }
    if relationship.target.node_id == local_node {
        drive(
            DeleteSyncRelationshipOperation::new(
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
) -> Result<RepairScan, SyncMirrorRepairError> {
    let mut records = Vec::new();
    let mut next_due_at_ms: Option<u64> = None;
    let mut start_after = None;
    loop {
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: SYNC_MIRROR_REPAIR_KEYSPACE.to_string(),
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
            other => return Err(SyncMirrorRepairError::Unexpected(other)),
        };

        for (key, value) in values {
            let record = match SyncMirrorRepairRecord::from_bytes(&key, &value) {
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
                next_due_at_ms = Some(
                    next_due_at_ms
                        .map_or(record.due_at_ms, |current| current.min(record.due_at_ms)),
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
        next_due_at_ms,
    })
}

async fn next_repair_after(
    storage: &StorageHandle,
) -> Result<Option<Duration>, SyncMirrorRepairError> {
    let now_ms = unix_timestamp_millis();
    let scan = scan_repair_records(storage, now_ms).await?;
    if !scan.records.is_empty() || scan.has_more_due {
        return Ok(Some(Duration::ZERO));
    }
    Ok(scan
        .next_due_at_ms
        .map(|due_at| Duration::from_millis(due_at.saturating_sub(now_ms))))
}

async fn reschedule_repair_record(
    storage: &StorageHandle,
    key: Vec<u8>,
    record: &SyncMirrorRepairRecord,
    error: String,
) -> Result<(), SyncMirrorRepairError> {
    let attempts = record.attempts.saturating_add(1);
    let next = SyncMirrorRepairRecord {
        relationship: record.relationship.clone(),
        intent: record.intent,
        due_at_ms: unix_timestamp_millis().saturating_add(queue_retry_after_ms(attempts)),
        attempts,
        last_error: Some(error),
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
                key_space: SYNC_MIRROR_REPAIR_KEYSPACE.to_string(),
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
            other => Err(SyncMirrorRepairError::Unexpected(other)),
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
    record: SyncMirrorRepairRecord,
) -> Result<(), SyncMirrorRepairError> {
    let key = record.relationship.id.to_bytes().to_vec();
    let txn_id = start_repair_transaction(storage).await?;
    let result = async {
        let existing = read_repair_record(storage, &key, Some(txn_id)).await?;
        if record.intent == SyncMirrorRepairIntent::Reconcile
            && existing.is_some_and(|existing| existing.intent == SyncMirrorRepairIntent::Delete)
        {
            return commit_repair_transaction(storage, txn_id).await;
        }
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: SYNC_MIRROR_REPAIR_KEYSPACE.to_string(),
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
            other => Err(SyncMirrorRepairError::Unexpected(other)),
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
    expected: SyncMirrorRepairIntent,
) -> Result<(), SyncMirrorRepairError> {
    let txn_id = start_repair_transaction(storage).await?;
    let result = async {
        let existing = read_repair_record(storage, &key, Some(txn_id)).await?;
        if existing.is_none_or(|existing| {
            existing.intent != expected
                || (expected == SyncMirrorRepairIntent::Reconcile
                    && existing.relationship != *relationship)
        }) {
            return commit_repair_transaction(storage, txn_id).await;
        }
        match storage
            .send_storage_effect(StorageEffect::Delete {
                key_space: SYNC_MIRROR_REPAIR_KEYSPACE.to_string(),
                key: ByteView::from(key),
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::DeleteResult { .. }) => {
                commit_repair_transaction(storage, txn_id).await
            }
            Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
            other => Err(SyncMirrorRepairError::Unexpected(other)),
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
    expected: &SyncMirrorRepairRecord,
) -> Result<(), SyncMirrorRepairError> {
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
                key_space: SYNC_MIRROR_REPAIR_KEYSPACE.to_string(),
                key: ByteView::from(key),
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::DeleteResult { .. }) => {
                commit_repair_transaction(storage, txn_id).await
            }
            Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
            other => Err(SyncMirrorRepairError::Unexpected(other)),
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
) -> Result<Option<SyncMirrorRepairRecord>, SyncMirrorRepairError> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: SYNC_MIRROR_REPAIR_KEYSPACE.to_string(),
            key: ByteView::from(key.to_vec()),
            txn_id,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => SyncMirrorRepairRecord::from_bytes(key, &value)
            .map(Some)
            .map_err(Into::into),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(SyncMirrorRepairError::Unexpected(other)),
    }
}

async fn read_out_relationship(
    storage: &StorageHandle,
    key: &[u8],
    txn_id: Option<TxnId>,
) -> Result<Option<SyncRelationship>, SyncMirrorRepairError> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: SYNC_RELATIONSHIP_OUT_KEYSPACE.to_string(),
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
        other => Err(SyncMirrorRepairError::Unexpected(other)),
    }
}

fn same_relationship(left: &SyncRelationship, right: &SyncRelationship) -> bool {
    left.id == right.id
        && left.source == right.source
        && left.target == right.target
        && left.mode == right.mode
        && left.replicate_deletes == right.replicate_deletes
        && left.created_by == right.created_by
        && left.created_at == right.created_at
}

async fn start_repair_transaction(storage: &StorageHandle) -> Result<TxnId, SyncMirrorRepairError> {
    match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => Ok(txn_id),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(SyncMirrorRepairError::Unexpected(other)),
    }
}

async fn commit_repair_transaction(
    storage: &StorageHandle,
    txn_id: TxnId,
) -> Result<(), SyncMirrorRepairError> {
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(SyncMirrorRepairError::Unexpected(other)),
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
) -> Result<(), SyncMirrorRepairError> {
    match storage
        .send_storage_effect(StorageEffect::Delete {
            key_space: SYNC_MIRROR_REPAIR_KEYSPACE.to_string(),
            key: ByteView::from(key),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(SyncMirrorRepairError::Unexpected(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{ArunaArn, RealmId, SyncMode, SyncState, SyncStatusSnapshot};
    use aruna_core::types::UserId;
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
            replicate_deletes: true,
            created_by: UserId::local(Ulid::from_bytes([5; 16]), realm_id),
            created_at: SystemTime::UNIX_EPOCH,
            state: SyncState::Enabled,
            status: SyncStatusSnapshot::default(),
        }
    }

    #[test]
    fn repair_record_roundtrips() {
        let record = SyncMirrorRepairRecord::new(relationship(), SyncMirrorRepairIntent::Reconcile);
        let key = record.relationship.id.to_bytes();

        assert_eq!(
            SyncMirrorRepairRecord::from_bytes(&key, &record.to_bytes().unwrap()).unwrap(),
            record
        );
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
                key_space: SYNC_MIRROR_REPAIR_KEYSPACE.to_string(),
                key: relationship.id.to_bytes().to_vec().into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing mirror repair record");
        };
        let record =
            SyncMirrorRepairRecord::from_bytes(&relationship.id.to_bytes(), &value).unwrap();
        assert_eq!(record.intent, SyncMirrorRepairIntent::Delete);
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
            StoreSyncRelationshipOperation::new(
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
                GetSyncRelationshipOperation::new(
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
            StoreSyncRelationshipOperation::new(
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
                GetSyncRelationshipOperation::new(
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

    #[tokio::test]
    async fn stale_reconcile_survives() {
        let tempdir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let relationship = relationship();
        let mut stale =
            SyncMirrorRepairRecord::new(relationship.clone(), SyncMirrorRepairIntent::Reconcile);
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
            SyncMirrorRepairRecord::new(current.clone(), SyncMirrorRepairIntent::Reconcile),
        )
        .await
        .unwrap();

        clear_mirror_repair(&storage, &stale, SyncMirrorRepairIntent::Reconcile)
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
            SyncMirrorRepairRecord::new(current, SyncMirrorRepairIntent::Delete),
        )
        .await
        .unwrap();

        clear_mirror_repair(&storage, &stale, SyncMirrorRepairIntent::Delete)
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
        drive(
            StoreSyncRelationshipOperation::new(
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

        let mut record =
            SyncMirrorRepairRecord::new(relationship.clone(), SyncMirrorRepairIntent::Reconcile);
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
                key_space: SYNC_MIRROR_REPAIR_KEYSPACE.to_string(),
                key: relationship.id.to_bytes().to_vec().into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing mirror repair retry");
        };
        let record =
            SyncMirrorRepairRecord::from_bytes(&relationship.id.to_bytes(), &value).unwrap();
        assert_eq!(record.attempts, 1);
        assert_eq!(record.last_error.as_deref(), Some("target unreachable"));
    }
}
