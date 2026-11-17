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
